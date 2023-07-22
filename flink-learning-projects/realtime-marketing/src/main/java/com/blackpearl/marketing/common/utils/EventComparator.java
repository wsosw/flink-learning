package com.blackpearl.marketing.common.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.blackpearl.marketing.common.pojo.EventLog;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;

public class EventComparator {

    public static boolean isConsistent(EventLog eventLog, JSONObject eventParam) throws ParseException {

        long eventTime = eventLog.getEventTime();

        String eventId = eventParam.getString("eventId");
        String windowStart = eventParam.getString("windowStart");
        String windowEnd = eventParam.getString("windowEnd");

        // 1. 如果当前事件ID和参数中ID不一致，说明不是当前事件，直接跳过本次循环
        if (!eventId.equals(eventLog.getEventId()))
            return false;

        // 2. 判断事件是否在规则约定的时间窗口内，如果没定义时间窗口，则默认满足；如果定义了时间窗口，则在时间窗口内才满足，否则直接跳过这次循环
        if (windowStart != null && !windowStart.isEmpty()) {
            long startTime = DateUtils.parseDate(windowStart, "yyyy-MM-dd HH:mm:ss").getTime();
            if (eventTime < startTime)
                return false;
        }

        if (windowEnd != null && !windowEnd.isEmpty()) {
            long endTime = DateUtils.parseDate(windowEnd, "yyyy-MM-dd HH:mm:ss").getTime();
            if (eventTime > endTime)
                return false;
        }

        // 3. 判断事件属性是否全部匹配
        // 目前只支持每个大事件的动态逻辑关系（自定义或与非逻辑关系），单个事件的多个属性条件只支持 && 的关系，
        // 如果想实现单事件中多属性的动态逻辑组合，可将其提取成一个父级事件条件即可。

        JSONArray attributeParams = eventParam.getJSONArray("attributeParams");
        for (int j = 0; j < attributeParams.size(); j++) {
            JSONObject attributeParam = attributeParams.getJSONObject(j);

            String attributeName = attributeParam.getString("attributeName");
            String compareType = attributeParam.getString("compareType");
            String compareValue = attributeParam.getString("compareValue");
            String eventAttributeValue = eventLog.getProperties().get(attributeName);

            if (eventAttributeValue == null) return false;
            if ("=".equals(compareType) && !(compareValue.compareTo(eventAttributeValue) == 0)) return false;
            if (">".equals(compareType) && !(compareValue.compareTo(eventAttributeValue) > 0)) return false;
            if ("<".equals(compareType) && !(compareValue.compareTo(eventAttributeValue) < 0)) return false;
            if (">=".equals(compareType) && !(compareValue.compareTo(eventAttributeValue) >= 0)) return false;
            if ("<=".equals(compareType) && !(compareValue.compareTo(eventAttributeValue) <= 0)) return false;

        }

        // 如果事件ID，时间窗口，属性名和属性值全部匹配，则说明当前事件与事件定义参数是一致的
        return true;

    }

}
