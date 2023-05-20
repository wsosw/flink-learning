package com.blackpearl.examples;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class _07_SideOutput {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);
        env.setParallelism(1);


        DataStreamSource<EventLog> source = env.addSource(new MySourceFunction());


        // 定义一个测流输出标签，泛型为测流输出数据的格式，它与主流输出的数据格式可以不一致
        OutputTag<String> outputTag = new OutputTag<String>("put-back-side", TypeInformation.of(String.class));

        // ProcessFunction<EventLog, String>中的泛型String是主流输出数据的格式，主流输出格式和测流输出格式可以不一致
        SingleOutputStreamOperator<String> processStream = source.process(new ProcessFunction<EventLog, String>() {
            @Override
            public void processElement(EventLog eventLog, ProcessFunction<EventLog, String>.Context context, Collector<String> collector) throws Exception {

                if (eventLog.getEventId().equals("putBack")) {

                    context.output(outputTag, JSON.toJSONString(eventLog));

                } else {

                    collector.collect(JSON.toJSONString(eventLog));
                }
            }
        });


        DataStream<String> sideStream = processStream.getSideOutput(outputTag);


        processStream.print("main ");
        sideStream.print("side ");


        env.execute("side-output");
    }

}
