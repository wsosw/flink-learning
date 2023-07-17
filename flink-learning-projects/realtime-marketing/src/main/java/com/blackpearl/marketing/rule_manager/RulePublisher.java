package com.blackpearl.marketing.rule_manager;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.blackpearl.marketing.common.interfaces.RuleCalculator;
import com.blackpearl.marketing.common.pojo.EventLog;
import com.blackpearl.marketing.rule_manager.service.ActionConditionQueryService;
import com.blackpearl.marketing.rule_manager.service.ProfileConditionQueryService;
import com.blackpearl.marketing.rule_model.calculator.java.RuleModelCalculator_01;
import org.roaringbitmap.RoaringBitmap;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.function.Consumer;

public class RulePublisher {

    public static void main(String[] args) throws IOException, SQLException, ParseException {

        String ruleStr = new String(Files.readAllBytes(Paths.get("D:\\WorkSpace\\Java\\flink-learning\\flink-learning-projects\\realtime-marketing\\src\\main\\java\\com\\blackpearl\\marketing\\rule_model\\rule1.json")));
        JSONObject ruleDefinition = JSON.parseObject(ruleStr);

        String ruleId = ruleDefinition.getString("ruleId");
        JSONArray profileCondition = ruleDefinition.getJSONArray("profileCondition");
        JSONObject actionCountCondition = ruleDefinition.getJSONObject("actionCountCondition");
        JSONArray eventParams = actionCountCondition.getJSONArray("eventParams");


        // 1. 取出用户画像条件，在ES中查询与画像条件匹配的用户，形成Bitmap

        System.out.println("人群圈选计算开始...");

        ProfileConditionQueryService profileConditionQueryService = new ProfileConditionQueryService();
        RoaringBitmap profileUserBitmap = profileConditionQueryService.queryProfileUsers(profileCondition);

        System.out.println("人群圈选计算结束，得到满足画像条件的人群Bitmap...");
        System.out.println("规则条件历史数据查询统计开始...");

        // 2. 取出行为次数条件，在doris中计算历史结果，写入到redis状态机中（为flink计算提供初始值）
        ActionConditionQueryService actionConditionQueryService = new ActionConditionQueryService();
        for (int i = 0; i < eventParams.size(); i++) {
            actionConditionQueryService.processActionCountCondition(eventParams.getJSONObject(i), ruleId);
        }

        System.out.println("规则条件历史数据查询发布完成...");

        // TODO 3. 处理groovy模板，生成当前规则对应的groovy脚本代码
        // 先测试一下java代码能不能运行，能运行再转换代码
        RuleCalculator calculator = new RuleModelCalculator_01();
        calculator.init(ruleDefinition, RoaringBitmap.bitmapOf(1, 2, 3));

        HashMap<String, String> hashMap = new HashMap<>();
        hashMap.put("pageId", "page001");
        calculator.calculate(new EventLog(1, "e1", hashMap, 1689594838000L));

        System.out.println("运算机计算逻辑执行完成");

        // TODO 4. 真正发布规则动作，把相关信息放到 mysql，供 flink cdc 实时抓取



    }

}
