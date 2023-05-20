package com.blackpearl.examples;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class _12_WaterMark {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<EventLog> beanSource = env.socketTextStream("node01", 9999).map(str -> JSON.parseObject(str, EventLog.class));


        // 构造一个watermark的生成策略对象（算法策略，及事件时间的抽取方法）
        WatermarkStrategy<EventLog> watermark = WatermarkStrategy
                .<EventLog>forMonotonousTimestamps() //不允许乱序
                //.<EventLog>forBoundedOutOfOrderness(Duration.ofMillis(1000)) //允许最大乱序1s
                .withTimestampAssigner(new SerializableTimestampAssigner<EventLog>() {
                    @Override
                    public long extractTimestamp(EventLog eventLog, long l) {
                        return eventLog.getTimeStamp();
                    }
                });

        // 一般是在source源头生成watermark，watermark会自动向下游算子传递
        SingleOutputStreamOperator<EventLog> beanSourceWithWatermark = beanSource.assignTimestampsAndWatermarks(watermark);


        // 虽然 watermark 设置了不允许乱序，但以下process算子不涉及窗口计算，所以乱序的数据仍然会被处理
        // 在窗口计算中，如果 watermark 设置了不允许乱序或最大乱序时长，那超过乱序时长的数据就会被丢弃掉（当然也可自己设置对乱序数据的处理逻辑，比如输出到测流等）
        SingleOutputStreamOperator<String> processed = beanSourceWithWatermark.process(new ProcessFunction<EventLog, String>() {
            @Override
            public void processElement(EventLog eventLog, ProcessFunction<EventLog, String>.Context context, Collector<String> collector) throws Exception {

                // 可利用 context 查看当前的 watermark 等上下文信息
                long processingTime = context.timerService().currentProcessingTime();   // 当前记录的处理时间
                long currentWatermark = context.timerService().currentWatermark();  // 当前的水位线
                long timeStamp = context.timestamp();   // 当前记录的时间，对于本程序来说 timeStamp == eventLog.timeStamp


                System.out.println("当前记录的处理时间：" + processingTime);
                System.out.println("当前时刻的水位线：" + currentWatermark);
                System.out.println("当前记录的事件时间：" + timeStamp);
                System.out.println("当前记录信息：" + JSON.toJSONString(eventLog));
            }
        });
        processed.print();


        env.execute("watermark");
    }


}
