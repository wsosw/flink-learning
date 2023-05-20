package com.blackpearl.examples;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class _13_WindowCalculationApi {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<EventLog> beanSource = env.socketTextStream("node01", 9999)
                .map(str -> JSON.parseObject(str, EventLog.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<EventLog>forBoundedOutOfOrderness(Duration.ofMillis(3000L)).withTimestampAssigner(new SerializableTimestampAssigner<EventLog>() {
                    @Override
                    public long extractTimestamp(EventLog eventLog, long l) {
                        return eventLog.getTimeStamp();
                    }
                }));

        /**
         * keyBy前的全局开窗：Non-Keyed Windows => windowAll
         */

        // 每十条数据触发一次窗口计算
        beanSource.countWindowAll(10).apply(new AllWindowFunction<EventLog, String, GlobalWindow>() {
            @Override
            public void apply(GlobalWindow globalWindow, Iterable<EventLog> iterable, Collector<String> collector) throws Exception {
                // 窗口计算逻辑
                for (EventLog eventLog : iterable) {
                    collector.collect(JSON.toJSONString(eventLog));
                }
            }
        });

        // 每进来两条数据，触发一次最近十条数据的窗口计算（窗口长度为10，滑动步长为2）
        beanSource.countWindowAll(10, 2).apply(new AllWindowFunction<EventLog, String, GlobalWindow>() {
            @Override
            public void apply(GlobalWindow globalWindow, Iterable<EventLog> iterable, Collector<String> collector) throws Exception {
                // 窗口计算逻辑
                for (EventLog eventLog : iterable) {
                    collector.collect(JSON.toJSONString(eventLog));
                }
            }
        });

        // 全局：事件时间滚动窗口
        beanSource.windowAll(TumblingEventTimeWindows.of(Time.seconds(20))).apply(new AllWindowFunction<EventLog, Integer, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<EventLog> iterable, Collector<Integer> collector) throws Exception {
                int count = 0;
                for (EventLog eventLog : iterable) {
                    count++;
                }
                collector.collect(count);
            }
        });

        // 全局：事件时间滑动窗口
        beanSource.windowAll(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(5)));

        // 全局：处理时间滚动窗口
        beanSource.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(20)));

        // 全局：处理时间滑动窗口
        beanSource.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(20), Time.seconds(5)));

        // 全局：会话窗口
        // 没有固定的窗口长度，可以以时间间隙判定是否为两个会话。比如间隔三十秒没来数据则认为是两个会话，超过这个时长，则触发一次窗口计算
        // ProcessingTimeSessionWindows.withDynamicGap() 应该是一个动态间隙，这个字段应存在于每条数据中。
        // 大致意思是，接受到这条数据后检查一下这个字段（比如为30秒），则超过30秒后触发窗口计算（每个会话的这个字段值很可能不同）
        beanSource.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(30)));




        /**
         * keyBy后针对每个key的开窗：Keyed Windows => window
         */

        // keyed windows 就是针对keyby后的每个key开窗口
        KeyedStream<EventLog, Long> keyedStream = beanSource.keyBy(EventLog::getGuid);

        keyedStream.countWindow(10);

        keyedStream.countWindow(10, 2);

        keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(20)));

        keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(5)));

        keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(20)));

        keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(20), Time.seconds(5)));

        keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)));




        env.execute("window-calculation-api");
    }


}
