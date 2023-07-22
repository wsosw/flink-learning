package com.blackpearl.marketing.rule_model.calculator.groovy

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import com.blackpearl.marketing.common.interfaces.RuleCalculator
import com.blackpearl.marketing.common.pojo.EventLog
import com.blackpearl.marketing.common.pojo.RuleInfo
import org.apache.commons.lang3.time.DateUtils
import org.roaringbitmap.RoaringBitmap
import redis.clients.jedis.Jedis

import java.text.ParseException

class RuleModel_01_Calculator implements RuleCalculator{

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

        long eventTime = eventLog.getEventTime();

        String ruleId = ruleDefinition.getString("ruleId");
        JSONObject actionCountCondition = ruleDefinition.getJSONObject("actionCountCondition");
        JSONArray eventParams = actionCountCondition.getJSONArray("eventParams");

        for (int i = 0; i < eventParams.size(); i++) {
            JSONObject eventParam = eventParams.getJSONObject(i);

            Integer conditionId = eventParam.getInteger("conditionId");
            String eventId = eventParam.getString("eventId");
            String windowStart = eventParam.getString("windowStart");
            String windowEnd = eventParam.getString("windowEnd");

            // 1. 如果当前事件ID和参数中ID不一致，说明不是当前事件，直接跳过本次循环
            if (!eventId.equals(eventLog.getEventId())) continue;

            // 2. 判断事件是否在规则约定的时间窗口内，如果没定义时间窗口，则默认满足；如果定义了时间窗口，则在时间窗口内才满足，否则直接跳过这次循环
            if (windowStart != null && !windowStart.isEmpty()) {
                long startTime = DateUtils.parseDate(windowStart, "yyyy-MM-dd HH:mm:ss").getTime();
                if (eventTime < startTime) continue;
            }

            if (windowEnd != null && !windowEnd.isEmpty()) {
                long endTime = DateUtils.parseDate(windowEnd, "yyyy-MM-dd HH:mm:ss").getTime();
                if (eventTime > endTime) continue;
            }

            // 3. 判断事件属性是否全部匹配
            // 目前只支持每个大事件的动态逻辑关系（自定义或与非逻辑关系），单个事件的多个属性条件只支持 && 的关系，
            // 如果想实现单事件中多属性的动态逻辑组合，可将其提取成一个父级事件条件即可。

            int matchCount = 0;
            JSONArray attributeParams = eventParam.getJSONArray("attributeParams");
            for (int j = 0; j < attributeParams.size(); j++) {
                JSONObject attributeParam = attributeParams.getJSONObject(j);

                String attributeName = attributeParam.getString("attributeName");
                String compareType = attributeParam.getString("compareType");
                String compareValue = attributeParam.getString("compareValue");
                String eventAttributeValue = eventLog.getProperties().get(attributeName);

                if (eventAttributeValue == null) break;
                if ("=" == compareType && !(compareValue == eventAttributeValue)) break;
                if (">" == compareType && !(compareValue > eventAttributeValue)) break;
                if ("<" == compareType && !(compareValue < eventAttributeValue)) break;
                if (">=" == compareType && !(compareValue >= eventAttributeValue)) break;
                if ("<=" == compareType && !(compareValue <= eventAttributeValue)) break;

                matchCount++;
            }

            // 以上条件都满足（即当前事件与规则参数事件ID匹配，时间窗口也匹配，且所有属性参数也匹配），对redis中当前规则当前条件当前用户的次数+1
            if (matchCount == attributeParams.size()) {
                String redisKey = ruleId + ":" + conditionId;
                jedis.hincrBy(redisKey, String.valueOf(eventLog.getGuid()), 1);
            }

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
