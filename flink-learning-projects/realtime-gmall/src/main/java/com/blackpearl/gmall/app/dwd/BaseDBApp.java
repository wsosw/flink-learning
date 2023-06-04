package com.blackpearl.gmall.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.blackpearl.gmall.bean.TableProcess;
import com.blackpearl.gmall.common.MySqlConfig;
import com.blackpearl.gmall.function.CustomDeserializer;
import com.blackpearl.gmall.function.DimHbaseSinkFunction;
import com.blackpearl.gmall.function.TableProcessFunction;
import com.blackpearl.gmall.utils.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

public class BaseDBApp {

    public static final String TOPIC_ODS_BASE_DB = "ods_base_db";
    public static final String GROUP_BASE_DB_APP = "base_db_app";

    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 消费kafka中 ods_base_db 主题数据
        DataStreamSource<String> kafkaBaseDBStream = env.fromSource(
                KafkaUtil.getKafkaSource(TOPIC_ODS_BASE_DB, GROUP_BASE_DB_APP),
                WatermarkStrategy.noWatermarks(),
                "kafka-base-db-stream"
        );

        // 3. 将每行数据转换成json对象并过滤delete数据
        SingleOutputStreamOperator<JSONObject> jsonDBStream = kafkaBaseDBStream.map(JSON::parseObject).filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return !MySqlConfig.RECORD_OPERATION_DELETE.equals(jsonObject.getString("operation"));
            }
        });

        // 4. 使用FlinkCDC消费配置表并处理成广播流
        // 创建一个新的库和表，用来存储gmall库中表的分流信息
        MySqlSource<String> tableConf = MySqlSource.<String>builder()
                .hostname("node01")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-rtp")
                .tableList("gmall-rtp.table_process")
                .deserializer(new CustomDeserializer())
                .startupOptions(StartupOptions.latest())
                .build();
        DataStreamSource<String> tableConfStream = env.fromSource(tableConf, WatermarkStrategy.noWatermarks(), "table-conf");
        MapStateDescriptor<String, TableProcess> tableProcessMapStateDescriptor = new MapStateDescriptor<>("tbl-conf-desc", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableConfStream.broadcast(tableProcessMapStateDescriptor);

        // 5. 连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonDBStream.connect(broadcastStream);

        // 6. 分流处理数据 广播流数据和主流数据（根据广播流进行处理）
        OutputTag<JSONObject> hbaseTag = new OutputTag<>("hbase-tag", TypeInformation.of(JSONObject.class));
        SingleOutputStreamOperator<JSONObject> kafkaStream = connectedStream.process(new TableProcessFunction(hbaseTag, tableProcessMapStateDescriptor));

        // 7. 提取kafka流数据和hbase流数据
        // 以上分流结果中主流kafkaStream就是要写到kafka中的数据
        DataStream<JSONObject> hbaseStream = kafkaStream.getSideOutput(hbaseTag);

        // 8. 将kafka数据写入kafka主题，将hbase数据写入phoenix表
        kafkaStream.print("kafka数据：");
        hbaseStream.print("hbase数据：");

        // 分流数据写入目标系统
        hbaseStream.addSink(new DimHbaseSinkFunction());
        kafkaStream.sinkTo(KafkaSink.<JSONObject>builder()
                .setBootstrapServers(KafkaUtil.BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<JSONObject>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, KafkaSinkContext kafkaSinkContext, Long aLong) {
                        return new ProducerRecord<>(
                                jsonObject.getString("sinkTable"),
                                jsonObject.getString("after").getBytes()
                        );
                    }
                })
                .build()
        );

        // 9. 任务执行
        env.execute("base-db-app");
    }

}
