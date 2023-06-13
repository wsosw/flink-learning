package com.blackpearl.gmall.app.dwm;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.blackpearl.gmall.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * TODO 未测试
 * 需求：统计用户的跳出明细（即用户进入应用后，只访问了一个页面后就退出，不再继续访问其他页面）
 * 问题：怎样识别跳出行为呢？
 * 思路：把用户访问的最后一个页面识别出来
 * (1) 判断日志中是否存在上一个访问页面（last_page_id ?= null），如果为空，则说明这是本次进入应用后访问的第一个页面
 * (2) 首次访问后过一段时间有没有访问其他页面（判断下一条数据，如果上一次行为是跳出行为的话，则存在超时和last_page_id == null两种情况）
 */

public class UserJumpDetailApp {

    public static final String TOPIC_DWD_PAGE_LOG = "dwd_page_log";
    public static final String GROUP_USER_JUMP_DETAIL_APP = "user_jump_detail_app";
    public static final String TOPIC_DWM_USER_JUMP_DETAIL = "dwm_user_jump_detail";

    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取kafka中的 dwd_page_log 主题中的数据
        DataStreamSource<String> pageLogeStream = env.fromSource(
                KafkaUtil.getKafkaSource(TOPIC_DWD_PAGE_LOG, GROUP_USER_JUMP_DETAIL_APP),
                WatermarkStrategy.noWatermarks(),
                "user-jump-detail-app"
        );

        // 3. 数据格式转换 String -> JSONObject，并提取时间戳生成Watermark
        SingleOutputStreamOperator<JSONObject> pageLogJsonStream = pageLogeStream.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        })
                );

        // 4. 定义模式序列（获取能够陪陪上该模式的数据，不包括超时数据）
        // TODO 只是明白是什么意思，CEP还有待深入了解
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() == 0;
                    }
                }).times(2) // 出现两次last_page_id为空的数据
                .consecutive() // 且两次数据必须是相邻的
                .within(Time.seconds(10)); // 设置超时时间为10s，即超过十秒后算是重新进入应用

        // 5. 将以上模式序列匹配到数据流上
        PatternStream<JSONObject> patternStream = CEP.pattern(pageLogJsonStream.keyBy(json -> json.getJSONObject("common").getString("mid")), pattern);

        // 6. 提取匹配上的超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<>("time-out", TypeInformation.of(JSONObject.class));

        SingleOutputStreamOperator<JSONObject> selectStream = patternStream.select(timeOutTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("start").get(0); // 获取匹配超时数据中的第一条日志
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0); // 获取模式匹配数据的第一条日志
            }
        });

        // 7. 合并两种事件
        DataStream<JSONObject> timeOutStream = selectStream.getSideOutput(timeOutTag);
        DataStream<JSONObject> unionStream = selectStream.union(timeOutStream);

        // 8. 将数据写入到kafka
        unionStream.print();
        unionStream.map(JSON::toJSONString).sinkTo(KafkaUtil.getKafkaSink(TOPIC_DWM_USER_JUMP_DETAIL));

        // 9. 任务执行
        env.execute("user-jump-detail-app");

    }

}
