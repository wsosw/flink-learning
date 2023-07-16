package com.blackpearl.examples;

import org.apache.directory.shared.kerberos.codec.apRep.actions.ApRepInit;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class _16_TransformOperator {

    // map flatmap filter keyBy reduce ...

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EventLog event1 = new EventLog(1, "s1", "e1", 1000, null);
        EventLog event2 = new EventLog(2, "s1", "e2", 2000, null);
        EventLog event3 = new EventLog(3, "s2", "e1", 3000, null);
        EventLog event4 = new EventLog(4, "s1", "e1", 4000, null);
        EventLog event5 = new EventLog(5, "s1", "e2", 5000, null);
        EventLog event6 = new EventLog(6, "s2", "e1", 6000, null);

        List<EventLog> eventLogs = Arrays.asList(event1, event2, event3, event4, event5, event6);

        // keyBy
        env.fromCollection(eventLogs).keyBy(new KeySelector<EventLog, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(EventLog eventLog) throws Exception {
                return Tuple2.of(eventLog.getSessionId(), eventLog.getEventId());
            }
        }).minBy("guid").print();


        env.execute("transform-operator");
    }


}
