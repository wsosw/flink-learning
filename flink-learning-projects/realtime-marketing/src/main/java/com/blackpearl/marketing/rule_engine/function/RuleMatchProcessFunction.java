package com.blackpearl.marketing.rule_engine.function;

import com.alibaba.fastjson.JSON;
import com.blackpearl.marketing.common.interfaces.RuleCalculator;
import com.blackpearl.marketing.common.pojo.EventLog;
import com.blackpearl.marketing.common.pojo.RuleInfo;
import com.blackpearl.marketing.rule_engine.pojo.RuleMatchResult;
import com.blackpearl.marketing.rule_engine.utils.FlinkStateDescriptors;
import groovy.lang.GroovyClassLoader;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class RuleMatchProcessFunction extends KeyedBroadcastProcessFunction<Integer, EventLog, RuleInfo, String> {

    /**
     * 事件处理，规则运算机调用核心逻辑
     */
    @Override
    public void processElement(EventLog eventLog, KeyedBroadcastProcessFunction<Integer, EventLog, RuleInfo, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
        ReadOnlyBroadcastState<String, RuleInfo> ruleInfoBroadcastState = readOnlyContext.getBroadcastState(FlinkStateDescriptors.ruleInfoMapStateDescriptor);

        // 每个事件进来都要对所有上线规则进行匹配计算
        Iterable<Map.Entry<String, RuleInfo>> ruleEntries = ruleInfoBroadcastState.immutableEntries();
        for (Map.Entry<String, RuleInfo> ruleEntry : ruleEntries) {

            // 取出当前规则信息对象
            RuleInfo ruleInfo = ruleEntry.getValue();

            // 取出当前规则的运算机，对流入事件进行计算和处理
            RuleCalculator calculator = ruleInfo.getRuleCalculator();

            // TODO 判断当前规则是否需要开启定时器功能（对使用定时器和使用启定时器的规则需要进行不同的处理）
            calculator.calculate(eventLog);

            // TODO 收集calculator返回的数据，使用collector写出去
            // collector.collect();

        }


    }

    @Override
    public void processBroadcastElement(RuleInfo ruleInfo, KeyedBroadcastProcessFunction<Integer, EventLog, RuleInfo, String>.Context context, Collector<String> collector) throws Exception {

        BroadcastState<String, RuleInfo> ruleInfoBroadcastState = context.getBroadcastState(FlinkStateDescriptors.ruleInfoMapStateDescriptor);

        String operationType = ruleInfo.getOperationType();

        // 新增和修改是能够合并的，因为在抓取到修改规则信息时，规则管理平台按逻辑来讲已经把redis中的历史状态信息重新计算一遍
        if ("UPDATE".equals(operationType) || "INSERT".equals(operationType)) {
            // 动态加载编译当前规则的groovy运算机代码，并反射成具体的运算机对象
            Class aClass;
            try (GroovyClassLoader groovyClassLoader = new GroovyClassLoader()) {
                aClass = groovyClassLoader.parseClass(ruleInfo.getRuleCalculatorCode());
            }
            RuleCalculator ruleCalculator = (RuleCalculator) aClass.newInstance();

            // 对规则运算机执行初始化操作，并将运算机对象加载到当前规则信息中
            ruleCalculator.init(ruleInfo);
            ruleInfo.setRuleCalculator(ruleCalculator);

            // 将当前规则信息放入状态中
            ruleInfoBroadcastState.put(ruleInfo.getRuleId(), ruleInfo);
        }

        // 如果操作类型为删除规则，则将当前规则信息从广播状态中删除
        if ("DELETE".equals(operationType)) {
            ruleInfoBroadcastState.remove(ruleInfo.getRuleId());
        }

    }
}
