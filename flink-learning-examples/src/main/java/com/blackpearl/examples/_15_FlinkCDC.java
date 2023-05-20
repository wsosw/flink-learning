package com.blackpearl.examples;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class _15_FlinkCDC {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///f:/flink/ckpt");

        /**
         * 注意：
         *   1. 要考虑版本兼容问题，mysql-cdc-2.2 版本依赖的是 flink-1.13，如果使用1.14会不兼容（mysql驱动不匹配）
         *   2. mysql服务器用的版本是8.2.30，从8.0.26版本开始，mysql的驱动名称发生了改变，如果依赖中的mysql版本与服务器版本对应不上也会报错
         *      低版本的mysql-cdc不知道有没有使用mysql驱动器依赖，如果使用了，肯定也是低版本。
         * <p>
         *   mysql驱动依赖还是要和服务器中的mysql版本对应或兼容。
         */

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("node01")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("test")
                .tableList("test.user_info,test.event_log")
                .deserializer(new CustomDeserializer())
//                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> binlog = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-source");
        binlog.print();

        // 数据写入kafka
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("node01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic("tsto")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        binlog.sinkTo(kafkaSink);

        env.execute("flink-connector-mysql-cdc");
    }


    private static class CustomDeserializer implements DebeziumDeserializationSchema<String> {

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

            // 获取主题信息：库名、表名
            String topic = sourceRecord.topic();
            String[] split = topic.split("\\.");
            String dbName = split[1];
            String tableName = split[2];

            // 获取操作类型: READ DELETE CREATE UPDATE TRUNCATE
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);

            Struct value = (Struct) sourceRecord.value();
            // 获取变化前数据
            Struct before = value.getStruct("before");
            JSONObject beforeJson = new JSONObject();
            if (before != null) { // 要注意判空
                for (Field field : before.schema().fields()) {
                    Object o = before.get(field);
                    beforeJson.put(field.name(), o);
                }
            }

            // 获取变化后数据
            Struct after = value.getStruct("after");
            JSONObject afterJson = new JSONObject();
            if (after != null) { // 要注意判空
                for (Field field : after.schema().fields()) {
                    Object o = after.get(field);
                    afterJson.put(field.name(), o);
                }
            }

            // 创建自定义JSON对象
            JSONObject result = new JSONObject();
            result.put("database", dbName);
            result.put("table", tableName);
            result.put("operation", operation);
            result.put("before", beforeJson);
            result.put("after", afterJson);

            // 发送数据
            collector.collect(result.toJSONString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }

}
