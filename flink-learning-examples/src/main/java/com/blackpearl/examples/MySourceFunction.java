package com.blackpearl.examples;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;


/**
 * 1. SourceFunction: 单并行度，无 open(), close() 等方法，无法调用context上下文参数
 * 2. ParallelSourceFunction: 多并行度，无 open(), close() 等方法，可以法调用context上下文参数
 * 3. RichSourceFunction: 单并行度，有 open(), close() 等方法，无法调用context上下文参数
 * 4. RichParallelSourceFunction: 多并行度，有 open(), close() 等方法，可以调用context上下文参数
 */
public  class MySourceFunction implements SourceFunction<EventLog> {

    volatile boolean flag = true;

    @Override
    public void run(SourceContext<EventLog> context) throws Exception {
        EventLog eventLog = new EventLog();
        String[] events = {"appLaunch", "pageLoad", "adShow", "adClick", "itemShare", "itemCollect", "putBack", "wakeUp", "appClose"};
        HashMap<String, String> eventInfoMap = new HashMap<>();

        while (flag) {

            eventLog.setGuid(RandomUtils.nextLong(1, 10000));
            eventLog.setSessionId(RandomStringUtils.randomAlphabetic(12).toUpperCase());
            eventLog.setTimeStamp(System.currentTimeMillis());
            eventLog.setEventId(events[RandomUtils.nextInt(0, events.length)]);

            eventInfoMap.put(RandomStringUtils.randomAlphabetic(1).toUpperCase(), RandomStringUtils.randomAlphabetic(32));
            eventLog.setEventInfo(eventInfoMap);

            context.collect(eventLog);

            eventInfoMap.clear();

            Thread.sleep(RandomUtils.nextInt(200, 1500));
//            Thread.sleep(RandomUtils.nextInt(10, 100));
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}

//class MyRichSourceFunction extends RichSourceFunction<EventLog> {
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//
//        // ...
//        String taskName = getRuntimeContext().getTaskName();
//    }
//
//    @Override
//    public void run(SourceContext<EventLog> sourceContext) throws Exception {
//
//    }
//
//    @Override
//    public void cancel() {
//
//    }
//}
