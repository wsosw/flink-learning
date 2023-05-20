package com.blackpearl.examples;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class _10_BroadcastOperator {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         * 案例背景：数据打宽
         *
         * 假设有两个流：
         *     流1：用户行为事件 => 同一用户的行为信息可能反复出现
         *     流2：用户维度信息 => 同一用户的维度信息只出现一次（作为广播流）
         *
         * 需求：
         *     降用户维度信息填充到用户的行为事件信息中
         */

        // 行为事件流：id, eventId
        SingleOutputStreamOperator<Event> eventStream = env.socketTextStream("node01", 9998).map(str -> {
            String[] fields = str.split(",");
            return new Event(Integer.parseInt(fields[0]), fields[1]);
        });

        // 用户信息流：id, name, age
        SingleOutputStreamOperator<UserInfo> userStream = env.socketTextStream("node01", 9999).map(str -> {
            String[] fields = str.split(",");
            return new UserInfo(Integer.parseInt(fields[0]), fields[1], Integer.parseInt(fields[2]));
        });

        // 把用户信息流变成广播流
        MapStateDescriptor<Integer, UserInfo> userDescriptor = new MapStateDescriptor<>("UserInfo", Integer.class, UserInfo.class);
        BroadcastStream<UserInfo> userInfoBroadcastStream = userStream.broadcast(userDescriptor);

        // 行为事件流连接广播流
        SingleOutputStreamOperator<UserEvent> processed = eventStream.connect(userInfoBroadcastStream).process(new BroadcastProcessFunction<Event, UserInfo, UserEvent>() {
            @Override
            public void processElement(Event event, BroadcastProcessFunction<Event, UserInfo, UserEvent>.ReadOnlyContext readOnlyContext, Collector<UserEvent> collector) throws Exception {

                ReadOnlyBroadcastState<Integer, UserInfo> broadcastState = readOnlyContext.getBroadcastState(userDescriptor);

                // 其实 broadcastState == null 和 userInfo == null 判空条件可以写在一起，但为了更直观地体现判断逻辑的层次性，下面将两个判空条件分开写

                // 注意：broadcastState需要判空（可能行为事件流先进来数据，而此时用户信息流中还没有数据）
                if (broadcastState == null) {

                    collector.collect(new UserEvent(event.id, null, 0, event.eventId));

                } else {

                    UserInfo userInfo = broadcastState.get(event.id);

                    // 注意：当broadcastState不为空时，广播流中也不一定有当前用户的维度信息
                    if (userInfo == null) {

                        collector.collect(new UserEvent(event.id, null, 0, event.eventId));

                    } else {

                        collector.collect(new UserEvent(event.id, userInfo.name, userInfo.age, event.eventId));

                    }
                }
            }

            @Override
            public void processBroadcastElement(UserInfo userInfo, BroadcastProcessFunction<Event, UserInfo, UserEvent>.Context context, Collector<UserEvent> collector) throws Exception {

                // 在该方法中，处理广播流数据（用户信息）
                BroadcastState<Integer, UserInfo> userBroadcastState = context.getBroadcastState(userDescriptor);
                userBroadcastState.put(userInfo.id, userInfo);

            }
        });
        processed.map(JSON::toJSONString).print();



        env.execute("broadcast-operator");
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class UserEvent {
        private int id;
        private String name;
        private int age;
        private String eventId;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Event {
        private int id;
        private String eventId;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class UserInfo {
        private int id;
        private String name;
        private int age;
    }


}
