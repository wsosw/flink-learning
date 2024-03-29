package com.blackpearl.marketing.rule_model.calculator.java;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.blackpearl.marketing.common.interfaces.RuleCalculator;
import com.blackpearl.marketing.common.pojo.EventLog;
import com.blackpearl.marketing.common.pojo.RuleInfo;
import com.blackpearl.marketing.common.utils.EventComparator;
import org.apache.commons.lang3.time.DateUtils;
import org.roaringbitmap.RoaringBitmap;
import redis.clients.jedis.Jedis;

import java.text.ParseException;


/**
 * 规则模型_01运算机：该规则模型包含画像条件和行为次数条件
 * 运算器java代码只在开发测试时使用，生产中要将java代码转成groovy代码，并使用enjoy模板引擎处理，最后放到数据库中
 * 规则上线时，会自动到mysql中去查，然后渲染成对应的计算代码
 */
public class RuleModel_01_Calculator implements RuleCalculator {

    private Jedis jedis;
    private JSONObject ruleDefinition;
    private RoaringBitmap profileUserBitmap;

    @Override
    public void init(RuleInfo ruleInfo) {
        this.jedis = new Jedis("node01", 6379);
        this.ruleDefinition = JSON.parseObject(ruleInfo.getRuleDefinitionJson());
        this.profileUserBitmap = ruleInfo.getProfileUserBitmap();
    }

    @Override
    public void calculate(EventLog eventLog) throws ParseException {

        // TODO 目前规则模型一默认画像条件和行为次数条件是有值的，如果后期需要灵活处理，这里还要做判断
        // 如果当前用户是改规则的受众人群，则触发计算逻辑，否则什么也不做
        if (profileUserBitmap.contains(eventLog.getGuid())) {

            // 如果满足画像条件，先对事件做判断，如果满足行为次数条件参数（事件ID匹配，时间窗口匹配，且所有属性参数也匹配），则将redis中对应状态值+1
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

            // 如果画像条件满足，则还需判断当前规则是否包含触发事件 -> triggerEvent 字段是否有值
            // 如果存在触发事件（triggerEvent != null），则只有当前行为（eventLog）与触发事件一致时，才会触发 match() 方法
            // 如果没有触发事件（triggerEvent == null），则说明只要规则一满足，就需要向用户发送触达信息，这也意味着每次事件计算完成后就要调用一次 match() 方法
            JSONObject triggerEventParam = ruleDefinition.getJSONObject("triggerEvent");
            if (triggerEventParam == null || EventComparator.isConsistent(eventLog, triggerEventParam)) {

                match(eventLog.getGuid());


                // TODO 返回规则匹配触达信息

            }


            // TODO 返回规则不匹配信息

        }

    }

    @Override
    public boolean match(int guid) {

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

        // 模板代码
        // 这里的命名不太好，下边指令中的eventParams是enjoy模板引擎外部需要传入的参数，而不是上边具体的eventParams参数
        // #for(eventParam : eventParams)
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
        // return #(combineExpr);

        return false;
    }
}
