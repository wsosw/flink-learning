package com.blackpearl.gmall;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class KafkaUtil {

    public static final String BROKERS = "node01:9092,node02:9092,node03:9092";

    public static KafkaSink<String> getKafkaSink(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
    }

    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setTopics(topic)
                .setGroupId(groupId)
                .setBootstrapServers(BROKERS)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("auto.offset.commit", "true")
                .build();
    }

}
