package com.blackpearl.marketing.rule_manager;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.blackpearl.marketing.common.interfaces.RuleCalculator;
import com.blackpearl.marketing.common.pojo.EventLog;
import com.blackpearl.marketing.rule_manager.service.ActionConditionQueryService;
import com.blackpearl.marketing.rule_manager.service.RuleSystemMetaService;
import com.jfinal.template.Engine;
import com.jfinal.template.Template;
import groovy.lang.GroovyClassLoader;
import org.roaringbitmap.RoaringBitmap;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashMap;

public class Test_Calculator {

    public static void main(String[] args) throws IOException, SQLException, InstantiationException, IllegalAccessException, ParseException {

        String ruleStr = new String(Files.readAllBytes(Paths.get("D:\\WorkSpace\\Java\\flink-learning\\flink-learning-projects\\realtime-marketing\\src\\main\\java\\com\\blackpearl\\marketing\\rule_model\\rule1.json")));
        JSONObject ruleDefinition = JSON.parseObject(ruleStr);

        String ruleId = ruleDefinition.getString("ruleId");
        String ruleModelId = ruleDefinition.getString("ruleModelId");
        JSONArray profileCondition = ruleDefinition.getJSONArray("profileCondition");
        JSONObject actionCountCondition = ruleDefinition.getJSONObject("actionCountCondition");
        JSONArray eventParams = actionCountCondition.getJSONArray("eventParams");
        String combineExpr = actionCountCondition.getString("combineExpr");


        // 将规则模型模板代码正式渲染成当前规则对应运算代码
        Template template = Engine.use().getTemplate("D:\\WorkSpace\\Java\\flink-learning\\flink-learning-projects\\realtime-marketing\\src\\main\\java\\com\\blackpearl\\marketing\\rule_model\\calculator\\template\\RuleModelCalculator_01.template.enjoy");
        HashMap<String, Object> renderInfo = new HashMap<>();
        renderInfo.put("eventParams", eventParams);
        renderInfo.put("combineExpr", combineExpr);
        String calculatorCode = template.renderToString(renderInfo);


        // 测试运行时编译groovy代码并调用运算机相关功能
        Class aClass = new GroovyClassLoader().parseClass(calculatorCode);
        RuleCalculator calculator = (RuleCalculator) aClass.newInstance();

        RoaringBitmap profileUserBitmap = RoaringBitmap.bitmapOf(1, 2, 3);
        calculator.init(ruleDefinition, profileUserBitmap);
        HashMap<String, String> hashMap = new HashMap<>();
        hashMap.put("pageId", "page002");
        EventLog eventLog = new EventLog(1, "e3", hashMap, 1689667701975L);
        calculator.calculate(eventLog);

        System.out.println("规则条件是否满足：" + calculator.match(1));



    }



}
