package com.blackpearl.gmall.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.blackpearl.gmall.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * 启动日志格式：
 * {"common":{"ar":"370000","ba":"Redmi","ch":"web","is_new":"0","md":"Redmi k30","mid":"mid_731755","os":"Android 10.0","uid":"113","vc":"v2.1.132"},"start":{"entry":"icon","loading_time":9036,"open_ad_id":2,"open_ad_ms":8128,"open_ad_skip_ms":0},"ts":1683801343000}
 * 曝光日志格式：（曝光日志中也有page字段，也算页面日志）
 *{"common":{"ar":"370000","ba":"Redmi","ch":"web","is_new":"0","md":"Redmi k30","mid":"mid_731755","os":"Android 10.0","uid":"113","vc":"v2.1.132"},"displays":[{"display_type":"activity","item":"2","item_type":"activity_id","order":1,"pos_id":1},{"display_type":"activity","item":"2","item_type":"activity_id","order":2,"pos_id":1},{"display_type":"query","item":"21","item_type":"sku_id","order":3,"pos_id":2},{"display_type":"query","item":"27","item_type":"sku_id","order":4,"pos_id":1},{"display_type":"query","item":"20","item_type":"sku_id","order":5,"pos_id":4},{"display_type":"promotion","item":"20","item_type":"sku_id","order":6,"pos_id":5}],"page":{"during_time":18776,"page_id":"home"},"ts":1683801343000}
 * 页面日志格式：
 *{"common":{"ar":"110000","ba":"iPhone","ch":"Appstore","is_new":"1","md":"iPhone 8","mid":"mid_74344","os":"iOS 13.3.1","uid":"506","vc":"v2.1.134"},"page":{"during_time":11175,"last_page_id":"home","page_id":"search"},"ts":1683801344000}
 *
 */


public class BaseLogApp {

    public static final String TOPIC_ODS_BASE_LOG = "ods_base_log";
    public static final String GROUP_BASE_LOG_APP = "base_log_app";
    public static final String TOPIC_DWD_START_LOG = "dwd_start_log";
    public static final String TOPIC_DWD_DISPLAY_LOG = "dwd_display_log";
    public static final String TOPIC_DWD_PAGE_LOG = "dwd_page_log";



    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 消费 ods_base_log 主题数据
        DataStreamSource<String> kafkaOdsBaseLog = env.fromSource(
                KafkaUtil.getKafkaSource(TOPIC_ODS_BASE_LOG, GROUP_BASE_LOG_APP),
                WatermarkStrategy.noWatermarks(),
                "kafka_ods_base_log"
        );

        // 3. 把每行数据转换为JSON对象（这步还需要过滤脏数据，使用map不合适）
        OutputTag<String> dirtyLogTag = new OutputTag<>("dirty-log", Types.STRING);
        SingleOutputStreamOperator<JSONObject> jsonLogStream = kafkaOdsBaseLog.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                // 可能出现不完整的json
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    // 脏数据输出到测流
                    context.output(dirtyLogTag, s);
                }
            }
        });

        // 打印脏数据
        jsonLogStream.getSideOutput(dirtyLogTag).print("dirty-log: ");

        // 4. 新老用户校验（利用状态校验，使用common中的is_new字段判断，根据mid分组，后续数据中如果存在isnew=1的数据还需将字段值修正为0）
        SingleOutputStreamOperator<JSONObject> jsonLogStreamWithNewFlag = jsonLogStream
                .keyBy(json -> json.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    ValueState<String> userState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        userState = getRuntimeContext().getState(new ValueStateDescriptor<String>("user-state", Types.STRING));
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        if ("1".equals(isNew)) {

                            // 此处还要根据state判断是否需要修正is_new字段
                            if (userState.value() != null) {
                                jsonObject.getJSONObject("common").put("is_new", 0);
                            } else {
                                userState.update("1");
                            }
                        }
                        return jsonObject;
                    }
                });

        // 5. 日志数据分流，主流为页面日志，启动日志和曝光日志输出到测流
        OutputTag<String> startLogTag = new OutputTag<>("start-log", Types.STRING);
        OutputTag<String> displayLogTag = new OutputTag<>("display-log", Types.STRING);
        // 日志分流，需要调用context参数，因此需要使用process类型算子
        SingleOutputStreamOperator<String> pageLogStream = jsonLogStreamWithNewFlag.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                boolean isStartLog = jsonObject.containsKey("start");
                if (isStartLog) {
                    context.output(startLogTag, jsonObject.toJSONString());
                } else {
                    collector.collect(jsonObject.toJSONString());

                    if (jsonObject.containsKey("displays")) {

                        // 在曝光数据中插入pageID
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");

                        // 同一条数据中可能存在多条曝光记录，每条曝光记录都要输出
                        JSONArray displays = jsonObject.getJSONArray("displays");
                        for (Object display : displays) {
                            JSONObject jj = (JSONObject) display;
                            jj.put("page_id", pageId);
                            context.output(displayLogTag, jj.toJSONString());
                        }
                    }
                }
            }
        });

        // 6. 提取测流输出数据
        DataStream<String> startLogStream = pageLogStream.getSideOutput(startLogTag);
        DataStream<String> displayLogStream = pageLogStream.getSideOutput(displayLogTag);

        // 打印日志
        startLogStream.print("启动日志：");
        displayLogStream.print("曝光日志：");
        pageLogStream.print("页面日志：");


        // 7. 将三个流的数据输出到kafka中
        startLogStream.sinkTo(KafkaUtil.getKafkaSink(TOPIC_DWD_START_LOG));
        displayLogStream.sinkTo(KafkaUtil.getKafkaSink(TOPIC_DWD_DISPLAY_LOG));
        pageLogStream.sinkTo(KafkaUtil.getKafkaSink(TOPIC_DWD_PAGE_LOG));


        // 8. 任务执行
        env.execute("base-log-app");
    }


}
