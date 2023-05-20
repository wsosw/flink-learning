package com.blackpearl.examples;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class _08_StreamConnectOperator {


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         * 执行connect后的两个流仍然是独立的两个流，可以对这两个留分别进行处理
         * connect的好处是，可以让两个流共享状态信息：MapState，listState，ValueState（flink中的状态机制）
         */

        // connect: 两个数据流的数据类型可以不一致，但输出的数据类型要一一致
        DataStreamSource<String> strStream = env.fromElements("a", "b", "c");
        DataStreamSource<Integer> numStream = env.fromElements(1, 2, 3);

        strStream.connect(numStream).process(new CoProcessFunction<String, Integer, String>() {

            String prefix;

            @Override
            public void open(Configuration parameters) throws Exception {

                prefix = "xxxx";

            }

            @Override
            public void processElement1(String str, CoProcessFunction<String, Integer, String>.Context context, Collector<String> collector) throws Exception {
                collector.collect(prefix + "::" + str);
            }

            @Override
            public void processElement2(Integer num, CoProcessFunction<String, Integer, String>.Context context, Collector<String> collector) throws Exception {
                collector.collect(prefix + "::" + num.toString());
            }

        })/*.print()*/;


        // union: 两个数据流的数据类型必须一致
        // strStream.union(numStream); // 报错

        DataStreamSource<String> strStream2 = env.fromElements("d", "e", "f");
        strStream.union(strStream2)
                .map(String::toUpperCase)
                .print();



        env.execute("stream-connect-operator");
    }



}
