package com.blackpearl.marketing.rule_model.calculator.java;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.blackpearl.marketing.common.interfaces.RuleCalculator;
import com.blackpearl.marketing.common.pojo.EventLog;
import com.blackpearl.marketing.common.pojo.RuleInfo;
import com.blackpearl.marketing.common.utils.EventComparator;
import org.roaringbitmap.RoaringBitmap;
import redis.clients.jedis.Jedis;

import java.text.ParseException;

public class RuleModel_02_Calculator implements RuleCalculator {

    Jedis jedis;
    JSONObject ruleDefinition;
    RoaringBitmap profileUserBitmap;

    @Override
    public void init(RuleInfo ruleInfo) {
        this.jedis = new Jedis("node01", 6379);
        this.ruleDefinition = JSON.parseObject(ruleInfo.getRuleDefinitionJson());
        this.profileUserBitmap = ruleInfo.getProfileUserBitmap();
    }

    @Override
    public void calculate(EventLog eventLog) throws ParseException {

        // 首先判断是否为受众用户
        if (profileUserBitmap.contains(eventLog.getGuid())) {

            // 行为次数状态计算
            processEventWithActionCountCondition(eventLog);
            // 行为序列状态计算
            processEventWithActionSequenceCondition(eventLog);

            // 如果当前规则不存在触发事件，则说明只要一满足规则就马上推送消息，因此每次都需要match
            // 如果当前规则存在触发事件，则判断当前事件是否为触发事件，如果是则执行match，否则不执行match
            JSONObject triggerEventParam = ruleDefinition.getJSONObject("triggerEvent");
            if (triggerEventParam == null || EventComparator.isConsistent(eventLog, triggerEventParam)) {

                boolean match = match(eventLog.getGuid());

                // TODO 返回规则匹配触达信息

            }

        }

    }



    private void processEventWithActionCountCondition(EventLog eventLog) throws ParseException {

        // 先对事件做判断，如果满足行为次数条件参数（事件ID匹配，时间窗口匹配，且所有属性参数也匹配），则将redis中对应状态值+1
        String ruleId = ruleDefinition.getString("ruleId");
        JSONObject actionCountCondition = ruleDefinition.getJSONObject("actionCountCondition");
        JSONArray eventParams = actionCountCondition.getJSONArray("eventParams");

        for (int i = 0; i < eventParams.size(); i++) {
            JSONObject eventParam = eventParams.getJSONObject(i);
            if (EventComparator.isConsistent(eventLog, eventParam)) {
                Integer conditionId = eventParam.getInteger("conditionId");
                String redisKey = ruleId + ":" + conditionId;
                jedis.hincrBy(redisKey, String.valueOf(eventLog.getGuid()), 1);
            }
        }

    }

    private void processEventWithActionSequenceCondition(EventLog eventLog) throws ParseException {

        int guid = eventLog.getGuid();
        String ruleId = ruleDefinition.getString("ruleId");
        JSONObject actionSequenceCondition = ruleDefinition.getJSONObject("actionSequenceCondition");
        JSONArray eventParams = actionSequenceCondition.getJSONArray("eventParams");
        int conditionId = actionSequenceCondition.getInteger("conditionId");

        String redisSequenceStepKey = ruleId + ":" + conditionId + ":seq_step";
        String redisSequenceCountKey = ruleId + ":" + conditionId + ":seq_count";

        // 从redis中取出本规则对应当前用户的，待完成序列已到达的步骤号
        String stepStr = jedis.hget(redisSequenceStepKey, String.valueOf(guid));
        int step = stepStr == null ? 0 : Integer.parseInt(stepStr);

        // 取出行为序列参数中期待的下一个事件（注意：下一个事件的索引号就是step的值）
        JSONObject eventParam = eventParams.getJSONObject(step);
        // 判断当前时间是不是序列事件参数中期待的事件
        if (EventComparator.isConsistent(eventLog, eventParam)) {

            if (step == eventParams.size() - 1) {
                // 如果当前步骤已经到了序列事件中的最后一步，将redis中的步骤号重置为0
                jedis.hset(redisSequenceStepKey, String.valueOf(guid), "0");
                // 并且将redis中该用户该条件的完成次数+1
                jedis.hincrBy(redisSequenceCountKey, String.valueOf(guid), 1);
            } else {
                // 否则就只有步骤号+1
                jedis.hincrBy(redisSequenceStepKey, String.valueOf(guid), 1);
            }
        }

    }


    @Override
    public boolean match(int guid) {

        int maxMatchCount = ruleDefinition.getInteger("maxMatchCount");
        String ruleId = ruleDefinition.getString("ruleId");
        String redisMatchCountKey = ruleId + ":match_count";

        String realMatchCountStr = jedis.hget(redisMatchCountKey, String.valueOf(guid));
        int realMatchCount = realMatchCountStr == null ? 0 : Integer.parseInt(realMatchCountStr);

        if (realMatchCount < maxMatchCount) {

            boolean res_0 = matchActionCount(guid);
            boolean res_1 = matchActionSequence(guid);

            // 此处的表达式要用模板引擎渲染替换
            if (res_0 && res_1) {
                jedis.hincrBy(redisMatchCountKey, String.valueOf(guid), 1);
                return true;
            }

            // 模板引擎渲染
            // if (#(ruleCombineExpr)) {
            //     jedis.hincrBy(redisMatchCountKey, String.valueOf(guid), 1);
            //     return true;
            // }

        }

        return false;
    }

    private boolean matchActionCount(int guid) {

        // 从redis中查询指定用户当前规则的所有行为次数条件已完成的次数
        // 然后判断这些行为次数条件逻辑关系是否满足

        String ruleId = ruleDefinition.getString("ruleId");
        JSONObject actionCountCondition = ruleDefinition.getJSONObject("actionCountCondition");
        JSONArray eventParams = actionCountCondition.getJSONArray("eventParams");

        // 逻辑代码
        for (int i = 0; i < eventParams.size(); i++) {
            JSONObject eventParam = eventParams.getJSONObject(i);

            Integer conditionId = eventParam.getInteger("conditionId");
            Integer eventCount = eventParam.getInteger("eventCount");

            String redisKey = ruleId + ":" + conditionId;
            String currentCountStr = jedis.hget(redisKey, String.valueOf(guid));
            int currentCount = Integer.parseInt(currentCountStr == null ? "0" : currentCountStr);

            boolean res = currentCount >= eventCount;
        }

        return false;

        // 模板代码
        // 这里的命名不太好，下边指令中的eventParams是enjoy模板引擎外部需要传入的参数，而不是上边具体的eventParams参数
        // #for(actionCountEventParam : actionCountEventParams)
        // JSONObject eventParam_#(for.index) = eventParams.getJSONObject(#(for.index));
        //
        // Integer conditionId_#(for.index) = eventParam_#(for.index).getInteger("conditionId");
        // Integer eventCount_#(for.index) = eventParam_#(for.index).getInteger("eventCount");
        //
        // String redisKey_#(for.index) = ruleId + ":" + conditionId_#(for.index);
        // String currentCountStr_#(for.index) = jedis.hget(redisKey_#(for.index), String.valueOf(guid));
        // int currentCount_#(for.index) = Integer.parseInt(currentCountStr_#(for.index) == null ? "0" : currentCountStr_#(for.index));
        //
        // boolean res_#(for.index) = currentCount_#(for.index) >= eventCount_#(for.index);
        // #end
        //
        // return #(actionCountCombineExpr);

    }

    private boolean matchActionSequence(int guid) {

        String ruleId = ruleDefinition.getString("ruleId");
        JSONObject actionSequenceCondition = ruleDefinition.getJSONObject("actionSequenceCondition");
        int conditionId = actionSequenceCondition.getInteger("conditionId");
        Integer sequenceCountParam = actionSequenceCondition.getInteger("sequenceCount");

        String redisSequenceCountKey = ruleId + ":" + conditionId + ":seq_count";
        String realSequenceCountStr = jedis.hget(redisSequenceCountKey, String.valueOf(guid));
        int realSequenceCount = realSequenceCountStr == null ? 0 : Integer.parseInt(realSequenceCountStr);

        return realSequenceCount >= sequenceCountParam;
    }


}
