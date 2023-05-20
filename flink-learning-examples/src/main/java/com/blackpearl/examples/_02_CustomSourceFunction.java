package com.blackpearl.examples;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _02_CustomSourceFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 接收的参数是一个Source
        // env.fromSource();

        // 接收的参数是一个SourceFunction
        DataStreamSource<EventLog> source = env.addSource(new MySourceFunction());
        source.map(JSON::toJSONString).print();

        env.execute("custom-source-function");
    }

}