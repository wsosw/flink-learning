package com.blackpearl.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.LongValueSequenceIterator;

import java.util.Arrays;

/**
 * 从各种源头创建数据流
 */
public class _01_SourceOperator {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<Integer> elements = env.fromElements(1, 2, 3, 4, 5);
        elements.map(el -> el + 1)/*.print()*/;


        DataStreamSource<String> collection = env.fromCollection(Arrays.asList("aa", "bb", "cc"));
        collection.map(String::toUpperCase)/*.print()*/;


        // 注意：下边的fromParallelCollection算子的并行度是2，但print算子的并行度并不是2
        DataStreamSource<LongValue> longValueDataStreamSource = env.fromParallelCollection(new LongValueSequenceIterator(1, 10), LongValue.class).setParallelism(2);
        longValueDataStreamSource.map(lv -> lv.getValue() + 100)/*.print()*/;

        DataStreamSource<String> socketTextStream = env.socketTextStream("node01", 9999);
        // socketTextStream.print();

        DataStreamSource<String> textFile = env.readTextFile("./data/aaa.txt");
        // textFile.print();

        // 消费kafka中的数据
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setTopics("test01")
                .setGroupId("g1")
                .setBootstrapServers("node01:9092")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("auto.offset.commit", "true")
                .build();
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source").print();



        env.execute();
    }


}
