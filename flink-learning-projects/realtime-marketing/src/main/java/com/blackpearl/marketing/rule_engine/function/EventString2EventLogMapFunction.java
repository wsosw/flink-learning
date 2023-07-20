package com.blackpearl.marketing.rule_engine.function;

import com.alibaba.fastjson.JSON;
import com.blackpearl.marketing.common.pojo.EventLog;
import org.apache.flink.api.common.functions.MapFunction;

public class EventString2EventLogMapFunction implements MapFunction<String, EventLog> {
    @Override
    public EventLog map(String s) throws Exception {
        return JSON.parseObject(s, EventLog.class);
    }
}
