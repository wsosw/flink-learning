package com.blackpearl.gmall.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.blackpearl.gmall.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

/**
 * 需求：统计每日的活跃用户
 * 思路：根据页面日志中用户每天首次进入网站来计数
 */
public class UniqueVisitApp {

    public static final String TOPIC_DWD_PAGE_LOG = "dwd_page_log";
    public static final String GROUP_UNIQUE_VISIT_APP = "unique_visit_app";
    public static final String TOPIC_DWM_UNIQUE_VISIT = "dwm_unique_visit";

    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取kafka dwd_page_log 主题中的数据
        DataStreamSource<String> pageLogStream = env.fromSource(
                KafkaUtil.getKafkaSource(TOPIC_DWD_PAGE_LOG, GROUP_UNIQUE_VISIT_APP),
                WatermarkStrategy.noWatermarks(),
                "unique-visit-app"
        );

        // 3. 转换数据格式 String -> JSONObject
        SingleOutputStreamOperator<JSONObject> pageLogJsonStream = pageLogStream.map(JSON::parseObject);

        // 4. 过滤数据，只保留每个mid每天第一次登录的数据（状态编程）
        SingleOutputStreamOperator<JSONObject> uniqueVisitStream = pageLogJsonStream
                .keyBy(json -> json.getJSONObject("common").getString("mid"))
                .filter(new RichFilterFunction<JSONObject>() {

                    ValueState<String> dateValueState;
                    SimpleDateFormat simpleDateFormat;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("value-state", String.class);
                        // 设置日期状态的ttl为24小时
                        StateTtlConfig stateTtlConfig = new StateTtlConfig
                                .Builder(Time.hours(24))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build();
                        valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                        dateValueState = getRuntimeContext().getState(valueStateDescriptor);

                        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {

                        // 取出当前日志中上一个页面的ID
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");

                        // 注意：lastPageId不为空的情况有两种
                        // 一是当日打开网站并浏览了多个页面，
                        // 二是前一天浏览网站过了凌晨而并没有关闭，此时的用户活跃度并不能算在今天而应该算在前一天
                        // 因此，只有在当日存在lastPageId为空时，才说明当前用户在今天是活跃的
                        if (lastPageId == null || lastPageId.length() == 0) {

                            // 获取状态中当前用户的最后活跃日期（当然如果超过一个整天[0:00-24:00]没有活跃，该值为空）
                            String lastDate = dateValueState.value();
                            // 获取当前日志的时间戳
                            String currentDate = simpleDateFormat.format(jsonObject.getLong("ts"));
                            // 如果日期不相同，则更新状态值（此处并没有考虑长时间的数据乱序问题，尤其是跨越24:00的乱序数据）
                            if (!currentDate.equals(lastDate)) {
                                dateValueState.update(currentDate);
                                return true;
                            }
                        }
                        return false;
                    }
                });

        // 5. 将数据写入到kafka
        uniqueVisitStream.print();
        uniqueVisitStream
                .map(JSON::toJSONString)
                .sinkTo(KafkaUtil.getKafkaSink(TOPIC_DWM_UNIQUE_VISIT));


        // 6. 任务执行
        env.execute("unique-visit-app");

    }

}
