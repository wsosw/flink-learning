package com.blackpearl.marketing.rule_engine.utils;

import com.blackpearl.marketing.common.pojo.RuleInfo;
import org.apache.flink.api.common.state.MapStateDescriptor;

public class FlinkStateDescriptors {

    // <K:V> -> <ruleId:ruleInfo>
    public static final MapStateDescriptor<String, RuleInfo> ruleInfoMapStateDescriptor = new MapStateDescriptor<>("rule-map-state", String.class, RuleInfo.class);




}
