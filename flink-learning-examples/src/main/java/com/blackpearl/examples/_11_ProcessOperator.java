package com.blackpearl.examples;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class _11_ProcessOperator {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> socketTextStream = env.socketTextStream("node01", 9999);


        // 把数据给到process算子后，可对其进行任何形式的处理
        // ProcessFunction 继承自 RichFunction，因此可以调用或重写父类的生命周期方法（open，close等）
        // 还可以获取到运行时的上下文参数信息等（getRuntimeContext()）
        SingleOutputStreamOperator<String> processed = socketTextStream.process(new ProcessFunction<String, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {

                // 获取上下文信息
                String taskName = getRuntimeContext().getTaskName();

                super.open(parameters);
            }

            @Override
            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {

                // 可对数据进行任何形式的处理
                String str = s.toUpperCase();

                // 利用context对数据进行测流输出
                OutputTag<String> outputTag = new OutputTag<>("sideData", Types.STRING);
                context.output(outputTag, str);

                // 使用collector将数据从主流输出
                collector.collect(str);

            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });
        processed.print();


        env.execute("process-operator");
    }

}
