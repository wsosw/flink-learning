package com.blackpearl.marketing.rule_manager.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class ActionConditionQueryService {

    Connection connection;  // 负责查询doris中历史数据使用
    Jedis jedis;    // 负责将历史状态写入到redis状态机

    public ActionConditionQueryService() throws SQLException {
        connection = DriverManager.getConnection("jdbc:mysql://node01:9030/demo", "root", "123456");
        jedis = new Jedis("node01", 6379);
    }


    // 处理行为次数条件
    // 参数格式：
    // {
    //   "eventId": "e1",
    //   "attributeParams": [
    //     {
    //       "attributeName": "pageId",
    //       "compareType": "=",
    //       "compareValue": "page001"
    //     }
    //   ],
    //   "windowStart": "2022-08-01 12:00:00",
    //   "windowEnd": "2022-08-30 12:00:00",
    //   "eventCount": 3,
    //   "conditionId": 1,
    //   "dorisQueryTemplate": "action_count"
    // }
    public void processActionCountCondition(JSONObject eventParamJsonObject, String ruleId) throws SQLException {

        String eventId = eventParamJsonObject.getString("eventId");
        String windowStart = eventParamJsonObject.getString("windowStart");
        String windowEnd = eventParamJsonObject.getString("windowEnd");
        Integer conditionId = eventParamJsonObject.getInteger("conditionId");
        JSONArray attributeParamsJsonArray = eventParamJsonObject.getJSONArray("attributeParams");

        // 拼接查询SQL
        StringBuilder sql = new StringBuilder("select guid, count(1) as cnt from events_detail where 1=1 ");
        if (eventId != null && !eventId.isEmpty()) sql.append(String.format("and event_id = '%s' ", eventId));
        if (windowStart != null && !windowStart.isEmpty())
            sql.append(String.format("and event_time >= '%s' ", windowStart));
        if (windowEnd != null && !windowEnd.isEmpty()) sql.append(String.format("and event_time <= '%s' ", windowEnd));

        for (int i = 0; i < attributeParamsJsonArray.size(); i++) {
            String attributeName = attributeParamsJsonArray.getJSONObject(i).getString("attributeName");
            String compareType = attributeParamsJsonArray.getJSONObject(i).getString("compareType");
            String compareValue = attributeParamsJsonArray.getJSONObject(i).getString("compareValue");

            sql.append(String.format("and get_json_string(prop_json, '$.%s') %s '%s' ", attributeName, compareType, compareValue));
        }
        sql.append("group by guid");

        System.out.println("当前行为次数条件查询SQL：" + sql);

        // 构造redis写入参数
        String redisKey = ruleId + ":" + conditionId;
        HashMap<String, String> guidAndCount = new HashMap<>();

        ResultSet resultSet = connection.createStatement().executeQuery(sql.toString());
        while (resultSet.next()) {
            int guid = resultSet.getInt("guid");
            long count = resultSet.getLong("cnt");

            guidAndCount.put(String.valueOf(guid), String.valueOf(count));

            // 将数据分批次写入redis，分批次写入有以下两点考虑：1.一批全部写入可能导致内存消耗过大；2.一条条写入太过频繁，还有其他的规则计算也在实时读写
            if (guidAndCount.size() == 1000) {
                jedis.hmset(redisKey, guidAndCount);
                guidAndCount.clear();
            }
        }

        // 将最后不满的批次，写入到redis
        if (guidAndCount.size() > 0) {
            jedis.hmset(redisKey, guidAndCount);
        }

    }


    // 处理行为序列条件
    public void processActionSequenceCondition() {

    }


    public static void main(String[] args) throws SQLException {

        String actionCountParam = "{\n" +
                "  \"eventId\": \"e2\",\n" +
                "  \"attributeParams\": [\n" +
                "    {\n" +
                "      \"attributeName\": \"pageId\",\n" +
                "      \"compareType\": \"=\",\n" +
                "      \"compareValue\": \"page004\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"attributeName\": \"itemId\",\n" +
                "      \"compareType\": \"=\",\n" +
                "      \"compareValue\": \"item002\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"windowStart\": \"2022-08-01 12:00:00\",\n" +
//                "  \"windowEnd\": \"2022-08-30 12:00:00\",\n" +
                "  \"eventCount\": 3,\n" +
                "  \"conditionId\": 1,\n" +
                "  \"dorisQueryTemplate\": \"action_count\"\n" +
                "}";

        JSONObject jsonObject = JSON.parseObject(actionCountParam);


        ActionConditionQueryService service = new ActionConditionQueryService();
        service.processActionCountCondition(jsonObject, "rule001");

    }


}
