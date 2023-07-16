package com.blackpearl.marketing.rule_manager;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.blackpearl.marketing.rule_manager.service.ProfileConditionQueryService;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {

        String ruleInfo = "{\n" +
                "  \"ruleModelId\": 1,\n" +
                "  \"ruleId\": \"rule001\",\n" +
                "  \"profileCondition\": [\n" +
                "    {\n" +
                "      \"tagId\": \"tag01\",\n" +
                "      \"compareType\": \"eq\",\n" +
                "      \"compareValue\": \"3\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"actionCountCondition\": {\n" +
                "    \"eventParams\": [\n" +
                "      {\n" +
                "        \"eventId\": \"e1\",\n" +
                "        \"attributeParams\": [\n" +
                "          {\n" +
                "            \"attributeName\": \"pageId\",\n" +
                "            \"compareType\": \"=\",\n" +
                "            \"compareValue\": \"page001\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"windowStart\": \"2022-08-01 12:00:00\",\n" +
                "        \"windowEnd\": \"2022-08-30 12:00:00\",\n" +
                "        \"eventCount\": 3,\n" +
                "        \"conditionId\": 1,\n" +
                "        \"dorisQueryTemplate\": \"action_count\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"eventId\": \"e2\",\n" +
                "        \"attributeParams\": [\n" +
                "          {\n" +
                "            \"attributeName\": \"itemId\",\n" +
                "            \"compareType\": \"=\",\n" +
                "            \"compareValue\": \"item002\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"attributeName\": \"pageId\",\n" +
                "            \"compareType\": \"=\",\n" +
                "            \"compareValue\": \"page001\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"windowStart\": \"2022-08-01 12:00:00\",\n" +
                "        \"windowEnd\": \"2022-08-30 12:00:00\",\n" +
                "        \"eventCount\": 1,\n" +
                "        \"conditionId\": 2,\n" +
                "        \"dorisQueryTemplate\": \"action_count\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"eventId\": \"e3\",\n" +
                "        \"attributeParams\": [\n" +
                "          {\n" +
                "            \"attributeName\": \"pageId\",\n" +
                "            \"compareType\": \"=\",\n" +
                "            \"compareValue\": \"page002\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"windowStart\": \"2022-08-01 12:00:00\",\n" +
                "        \"windowEnd\": \"2022-08-30 12:00:00\",\n" +
                "        \"eventCount\": 1,\n" +
                "        \"conditionId\": 3,\n" +
                "        \"dorisQueryTemplate\": \"action_count\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"combineExpr\": \" res0 && (res1 || res2) \"\n" +
                "  }\n" +
                "}";

        JSONObject ruleInfoJson = JSON.parseObject(ruleInfo);
        JSONArray profileCondition = ruleInfoJson.getJSONArray("profileCondition");


        // 1. 取出用户画像条件，在ES中查询与画像条件匹配的用户，形成Bitmap

        System.out.println("人群圈选计算开始...");

        ProfileConditionQueryService profileConditionQueryService = new ProfileConditionQueryService();
        RoaringBitmap bitmap = profileConditionQueryService.queryProfileUsers(profileCondition);

        System.out.println("人群圈选计算结束，得到满足画像条件的人群Bitmap...");

        // 2. 取出行为次数条件，在doris中计算历史结果，写入到redis状态机中（为flink计算提供初始值）



    }

}
