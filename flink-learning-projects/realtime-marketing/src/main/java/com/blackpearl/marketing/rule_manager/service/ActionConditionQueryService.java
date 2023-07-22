package com.blackpearl.marketing.rule_manager.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.blackpearl.marketing.common.pojo.ActionSequenceParam;
import com.blackpearl.marketing.common.pojo.EventParam;
import com.jfinal.template.Engine;
import org.roaringbitmap.RoaringBitmap;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

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
    public void processActionCountCondition(JSONObject eventParamJsonObject, String ruleId, RoaringBitmap profileUserBitmap) throws SQLException {

        String eventId = eventParamJsonObject.getString("eventId");
        String windowStart = eventParamJsonObject.getString("windowStart");
        String windowEnd = eventParamJsonObject.getString("windowEnd");
        Integer conditionId = eventParamJsonObject.getInteger("conditionId");
        JSONArray attributeParamsJsonArray = eventParamJsonObject.getJSONArray("attributeParams");

        // TODO 弃用下边这种字符串拼接SQL的方式，不直观，后期改用enjoy模板渲染
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

            // 只将满足画像条件的人群的历史行为次数状态写入redis状态机
            if (profileUserBitmap.contains(guid)) {
                guidAndCount.put(String.valueOf(guid), String.valueOf(count));

                // 将数据分批次写入redis，分批次写入有以下两点考虑：1.一批全部写入可能导致内存消耗过大；2.一条条写入太过频繁，还有其他的规则计算也在实时读写
                if (guidAndCount.size() == 1000) {
                    jedis.hmset(redisKey, guidAndCount);
                    guidAndCount.clear();
                }
            }

        }

        // 将最后不满的批次，写入到redis
        if (guidAndCount.size() > 0) {
            jedis.hmset(redisKey, guidAndCount);
        }

    }


    // 处理行为序列条件
    public void processActionSequenceCondition(ActionSequenceParam actionSequenceParam, String ruleId, RoaringBitmap profileUserBitmap) throws SQLException {

        List<EventParam> eventParams = actionSequenceParam.getEventParams();

        HashMap<String, Object> renderData = new HashMap<>();
        renderData.put("windowStart", actionSequenceParam.getWindowStart());
        renderData.put("windowEnd", actionSequenceParam.getWindowEnd());
        renderData.put("eventParams", actionSequenceParam.getEventParams());

        String sqlTemplate = "select\n" +
                "    guid,\n" +
                "    group_concat(concat_ws('_',event_id,event_time),'^')\n" +
                "from events_detail\n" +
                "where 1=1\n" +
                "#if(windowStart != null)\n" +
                "and event_time >= '#(windowStart)'\n" +
                "#end\n" +
                "#if(windowEnd != null)\n" +
                "and event_time <= '#(windowEnd)'\n" +
                "#end\n" +
                "#if(eventParams.size > 0)\n" +
                "and\n" +
                "(\n" +
                "#end\n" +
                "  #for(eventParam: eventParams)\n" +
                "  (event_id = '#(eventParam.eventId)' #for(attrParam: eventParam.attributeParams) and get_json_string(propJson,'$.#(attrParam.attributeName)') #(attrParam.compareType) '#(attrParam.compareValue)'  #end)\n" +
                "  #if(for.last) #else OR #end\n" +
                "  #end\n" +
                "#if(eventParams.size > 0)\n" +
                ")\n" +
                "#end\n" +
                "group by guid";
        String sql = Engine.use().getTemplateByString(sqlTemplate).renderToString(renderData);

        System.out.println("当前行为序列条件查询SQL：" + sql);

        ResultSet resultSet = connection.createStatement().executeQuery(sql);
        while (resultSet.next()) {
            int guid = resultSet.getInt(1);
            // 如果当前用户是受众用户，才触发计算逻辑，否则什么也不做
            if (profileUserBitmap.contains(guid)) {

                //  3,"e2_2022-08-01 14:32:35,e3_2022-08-01 14:33:35,e1_2022-08-01 14:34:35"
                String guidAndEventStr = resultSet.getString(2);
                // [e2_2022-08-01 14:32:35^e3_2022-08-01 14:33:35^e1_2022-08-01 14:34:35]
                String[] eventIdAndEventTimeArray = guidAndEventStr.split("\\^");
                // 对以上结果按时间排序
                Arrays.sort(eventIdAndEventTimeArray, new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        String[] o1Split = o1.split("_");
                        String[] o2Split = o2.split("_");
                        return o1Split[1].compareTo(o2Split[1]);
                    }
                });

                int step = 0;   //当前完成到了序列中的第几步
                int matchCount = 0;    //当前序列完成了几次

                // 遍历排好序的行为序列
                for (String eventIdAndEventTime : eventIdAndEventTimeArray) {
                    if (eventIdAndEventTime.split("_")[0].equals(eventParams.get(step).getEventId())) {
                        step++;
                        if (step == eventParams.size()) {
                            step = 0;
                            matchCount++;
                        }
                    }
                }

                String redisSequenceStepKey = ruleId + ":" + actionSequenceParam.getConditionId() + ":seq_step";
                String redisSequenceCountKey = ruleId + ":" + actionSequenceParam.getConditionId() + ":seq_count";

                // 往redis插入该用户的待完成序列的已到达步骤号
                jedis.hset(redisSequenceStepKey, String.valueOf(guid), String.valueOf(step));
                // 往redis插入该用户的序列条件的已完成次数
                jedis.hset(redisSequenceCountKey, String.valueOf(guid), String.valueOf(matchCount));
            }


        }


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


//        ActionConditionQueryService service = new ActionConditionQueryService();
//        service.processActionCountCondition(jsonObject, "rule001");

    }


}
