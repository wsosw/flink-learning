package com.blackpearl.examples;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class _04_KafkaSinkOperator {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///f:/flink/ckpt");

        DataStreamSource<EventLog> source = env.addSource(new MySourceFunction());

//        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
//                .setBootstrapServers("node01:9092")
//                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
//                        .setTopic("test04")
//                        .setValueSerializationSchema(new SimpleStringSchema())
//                        .build())
//                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
//                .setTransactionalIdPrefix("xxx-")
//                .build();

        /**
         * KafkaSink 使用 DeliveryGuarantee.EXACTLY_ONCE报错
         * Exception: The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).
         * 参考: https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/connectors/datastream/kafka/#kafka-producer-%E5%92%8C%E5%AE%B9%E9%94%99
         *      https://blog.csdn.net/LangLang1111111/article/details/121395831
         */

        Properties props = new Properties();
        props.setProperty("transaction.timeout.ms", "900000");

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder() // 注意，此处有个泛型
                .setBootstrapServers("node01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic("test04")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 使用 EXACTLY_ONCE 时，设置前缀，区分事务
                .setTransactionalIdPrefix("xxx-")
                // new Properties() 封装 producer key-value 配置参数
                 .setKafkaProducerConfig(props)
                .build();

        source.map(JSON::toJSONString).sinkTo(kafkaSink);


        env.execute("kafka-sink-operator");
    }


}
