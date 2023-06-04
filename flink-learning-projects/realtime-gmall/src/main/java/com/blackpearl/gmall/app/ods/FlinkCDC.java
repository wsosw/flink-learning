package com.blackpearl.gmall.app.ods;

import com.blackpearl.gmall.function.CustomDeserializer;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://node01:8020/gmall/ckpt");

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
                .databaseList("gmall")
                .tableList(".*")
                .deserializer(new CustomDeserializer())
//                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.latest())
                .build();

        DataStreamSource<String> binlog = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-source");
        binlog.print();

        // 数据写入kafka
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("node01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic("ods_base_db")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        binlog.sinkTo(kafkaSink);

        env.execute("flink-connector-mysql-cdc");
    }


}
