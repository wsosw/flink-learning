package com.blackpearl.marketing.rule_engine.main;

import com.alibaba.fastjson.JSON;
import com.blackpearl.marketing.common.pojo.EventLog;
import com.blackpearl.marketing.rule_engine.function.EventString2EventLogMapFunction;
import com.blackpearl.marketing.rule_engine.function.RuleMatchProcessFunction;
import com.blackpearl.marketing.rule_engine.function.TableRow2RuleInfoMapFunction;
import com.blackpearl.marketing.common.pojo.RuleInfo;
import com.blackpearl.marketing.rule_engine.pojo.RuleMatchResult;
import com.blackpearl.marketing.rule_engine.utils.FlinkStateDescriptors;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 项目核心：flink实时规则计算引擎
 */
public class RuleEngine {

    public static void main(String[] args) throws Exception {

        // 创建编程环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/f:/flink/ckpt");
        env.setParallelism(1); // 测试并行度设置为1

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 消费kafka行为事件日志
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("node01:9092")
                .setTopics("rtmk-events")
                .setGroupId("rtmk-rule-engine")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 数据格式：{"guid":1,"eventId":"e1","properties":{"p1":"v1","p2":"2"},"eventTime":100000}
        // 转换数据格式：String -> EventLog
        SingleOutputStreamOperator<EventLog> eventLogStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-rtmk-events")
                .map(new EventString2EventLogMapFunction());

        eventLogStream.map(JSON::toJSONString).print("行为日志");

        // 使用flink-cdc抓取规则系统源数据库中的规则信息（包括新增规则，修改规则，删除规则）
        tenv.executeSql(" CREATE TABLE rule_info_cdc (     " +
                "   `id` int PRIMARY KEY NOT ENFORCED,        " +
                "   `rule_id` string,                         " +
                "   `rule_model_id` string,                   " +
                "   `rule_define_json` string,                " +
                "   `rule_profile_bitmap` binary,             " +
                "   `rule_calculator_code` string,            " +
                "   `rule_status` int,                        " +
                "   `rule_desc` string                        " +
                " ) WITH (                                    " +
                "   'connector' = 'mysql-cdc',                " +
                "   'hostname' = 'node01',                    " +
                "   'port' = '3306',                          " +
                "   'username' = 'root',                      " +
                "   'password' = '123456',                    " +
                "   'database-name' = 'rtmk',                 " +
                "   'table-name' = 'rule_info'                " +
                " )                                           ");
        Table table = tenv.sqlQuery("select * from rule_info_cdc");
        SingleOutputStreamOperator<RuleInfo> ruleInfoStream = tenv.toChangelogStream(table)
                .map(new TableRow2RuleInfoMapFunction())
                .filter(ruleInfo -> ruleInfo.getId() > 0);

        ruleInfoStream.map(JSON::toJSONString).print("规则信息");

        // 将规则信息流转换成广播流
        BroadcastStream<RuleInfo> ruleInfoBroadcastStream = ruleInfoStream.broadcast(FlinkStateDescriptors.ruleInfoMapStateDescriptor);

        // 连接事件流和广播流，并对每个流入的事件进行计算和处理
        SingleOutputStreamOperator<String> resultStream = eventLogStream
                .keyBy(EventLog::getGuid)
                .connect(ruleInfoBroadcastStream)
                .process(new RuleMatchProcessFunction());


        // TODO 将触达信息写入目标系统（比如kafka）
         resultStream.print();


        // 任务执行
        env.execute("rule-engine");
    }


}
