package com.blackpearl.examples;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;



/**
 *
 * 注意：
 *   1. watermark推进机制默认每200ms触发一次，数据流入的速度会对程序的处理产生影响。比如：
 *       一次性输入下边所有数据，由于watermark还没来得及推进，因此ts=6000的数据不算迟到数据
 *       如果按一定时间间隔输入每一条数据，当输入ts=6000的数据时，watermark已经推进到15000（实际为14999），并且这时0-19s的窗口已关闭，因此ts=6000的数据被认为是迟到数据
 *   2. 除以上因素外，多并行度也会对watermark的推进机制产生影响。因此，在多并行度测试环境下，测试结果也会不同。
 * <p>
 *
 * 测试数据：（以下数据均为在并行度为1的条件下，以不同的时间间隔输入）
 *   {"eventId":"e01","eventInfo":{"K":"MMM"},"guid":1,"sessionId":"DP","timeStamp":1000}
 *   {"eventId":"e02","eventInfo":{"K":"MMM"},"guid":2,"sessionId":"DP","timeStamp":2000}
 *   {"eventId":"e01","eventInfo":{"K":"MMM"},"guid":1,"sessionId":"DP","timeStamp":9000}
 *   {"eventId":"e01","eventInfo":{"K":"MMM"},"guid":2,"sessionId":"DP","timeStamp":3000} => 这条数据并不会处处到测流，因为该数据仍在当前窗口内，窗口计算并没有触发，因此该数据虽然迟到超过2s，但并不会被输出到测流
 *   {"eventId":"e01","eventInfo":{"K":"MMM"},"guid":1,"sessionId":"DP","timeStamp":10000} => 触发窗口计算（0-10s）
 *   {"eventId":"e01","eventInfo":{"K":"MMM"},"guid":1,"sessionId":"DP","timeStamp":11000}
 *   {"eventId":"e01","eventInfo":{"K":"MMM"},"guid":1,"sessionId":"DP","timeStamp":9000} => 因允许数据迟到2s，此处仍会触发上一个窗口（0-10s）的计算逻辑
 *   {"eventId":"e01","eventInfo":{"K":"MMM"},"guid":1,"sessionId":"DP","timeStamp":15000}
 *   {"eventId":"e01","eventInfo":{"K":"MMM"},"guid":1,"sessionId":"DP","timeStamp":6000} => 此时watermark=15000，且0-10s的窗口已关闭，迟到数据输出到测流
 *   {"eventId":"e01","eventInfo":{"K":"MMM"},"guid":2,"sessionId":"DP","timeStamp":18000}
 *   {"eventId":"e01","eventInfo":{"K":"MMM"},"guid":1,"sessionId":"DP","timeStamp":20000} => 触发窗口计算（10-20s）
 * <p>
 *
 * 输出结果：Tuple2.of(guid, eventCount)
 *   主流结果> (1,2)
 *   主流结果> (2,2)
 *   主流结果> (1,3)
 *   迟到数据> EventLog(guid=1, sessionId=DP, eventId=e01, timeStamp=6000, eventInfo={K=MMM})
 *   主流结果> (1,3)
 *   主流结果> (2,1)
 *
 */
public class _14_WindowCalculation {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         *
         * 数据的迟到和乱序处理可以从两个地方设置：
         *     1. forBoundedOutOfOrderness(Duration.ofMillis(3000L))   // watermark的推进机制中 watermark = 当前事件时间 - 3000ms - 1ms
         *     2. window(TimeWindow).allowedLateness(Time.seconds(2))  // 窗口计算中允许数据最多迟到两秒
         *
         */

        SingleOutputStreamOperator<EventLog> beanSource = env.socketTextStream("node01", 9999)
                .map(str -> JSON.parseObject(str, EventLog.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<EventLog>forBoundedOutOfOrderness(Duration.ofMillis(0)).withTimestampAssigner(new SerializableTimestampAssigner<EventLog>() {
                    @Override
                    public long extractTimestamp(EventLog eventLog, long l) {
                        return eventLog.getTimeStamp();
                    }
                }));

        // 将迟到的数据输出到测流
        OutputTag<EventLog> lateEventTag = new OutputTag<>("late-event-log", TypeInformation.of(EventLog.class));


        // Non-Keyed Window：数据流不经过 keyBy()
        // beanSource.windowAll()

        // Keyed Window: 数据流经过 keyBy()
        SingleOutputStreamOperator<Tuple2<Long, Integer>> aggregated = beanSource.keyBy(EventLog::getGuid)
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 窗口长度为10s的滚动窗口
                .allowedLateness(Time.seconds(2)) // 最大允许数据迟到2s
                // .evictor()  // 用于在窗口计算触发前后移除数据的方法，其实就是实现Evictor接口，重写evict方法，等用到时再研究吧
                .sideOutputLateData(lateEventTag)
                // AggregateFunction<> 中的三个泛型分别是：输出数据类型，累加器类型，输出数据类型
                .aggregate(new AggregateFunction<EventLog, Tuple2<Long, Integer>, Tuple2<Long, Integer>>() {
                    @Override
                    public Tuple2<Long, Integer> createAccumulator() {
                        return Tuple2.of(0L, 0);
                    }

                    @Override
                    public Tuple2<Long, Integer> add(EventLog eventLog, Tuple2<Long, Integer> accumulator) {
                        return Tuple2.of(eventLog.getGuid(), accumulator.f1 + 1);
                    }

                    @Override
                    public Tuple2<Long, Integer> getResult(Tuple2<Long, Integer> accumulator) {
                        return Tuple2.of(accumulator.f0, accumulator.f1);
                    }

                    @Override
                    public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> accA, Tuple2<Long, Integer> accB) {
                        return Tuple2.of(accA.f0, accA.f1 + accB.f1);
                    }
                });

        DataStream<EventLog> lateEventStream = aggregated.getSideOutput(lateEventTag);

        aggregated.print("主流结果");
        lateEventStream.print("迟到数据");




        env.execute("window-calculation");
    }


}
