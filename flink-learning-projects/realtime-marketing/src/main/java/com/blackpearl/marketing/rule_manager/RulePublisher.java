package com.blackpearl.marketing.rule_manager;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.blackpearl.marketing.rule_manager.service.ActionConditionQueryService;
import com.blackpearl.marketing.rule_manager.service.ProfileConditionQueryService;
import com.blackpearl.marketing.rule_manager.service.RuleSystemMetaService;
import com.jfinal.template.Engine;
import com.jfinal.template.Template;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashMap;

public class RulePublisher {

    public static void main(String[] args) throws IOException, SQLException, ParseException {

        String ruleStr = new String(Files.readAllBytes(Paths.get("D:\\WorkSpace\\Java\\flink-learning\\flink-learning-projects\\realtime-marketing\\src\\main\\java\\com\\blackpearl\\marketing\\rule_model\\rule1.json")));
        JSONObject ruleDefinition = JSON.parseObject(ruleStr);

        String ruleId = ruleDefinition.getString("ruleId");
        String ruleModelId = ruleDefinition.getString("ruleModelId");
        JSONArray profileCondition = ruleDefinition.getJSONArray("profileCondition");
        JSONObject actionCountCondition = ruleDefinition.getJSONObject("actionCountCondition");
        JSONArray eventParams = actionCountCondition.getJSONArray("eventParams");
        String combineExpr = actionCountCondition.getString("combineExpr");


        // 1. 取出用户画像条件，在ES中查询与画像条件匹配的用户，形成Bitmap
        ProfileConditionQueryService profileConditionQueryService = new ProfileConditionQueryService();
        RoaringBitmap profileUserBitmap = profileConditionQueryService.queryProfileUsers(profileCondition);

        // ES掌握的还不行，有时候会出问题，以下这行直接生成bitmap的代码仅在测试中使用
        // RoaringBitmap profileUserBitmap = RoaringBitmap.bitmapOf(1, 2, 3);

        // 2. 取出行为次数条件，在doris中计算历史结果，写入到redis状态机中（为flink计算提供初始值）
        ActionConditionQueryService actionConditionQueryService = new ActionConditionQueryService();
        for (int i = 0; i < eventParams.size(); i++) {
            actionConditionQueryService.processActionCountCondition(eventParams.getJSONObject(i), ruleId);
        }

        // 3. 查询当前规则对应模型的运算机模板代码
        RuleSystemMetaService ruleSystemMetaService = new RuleSystemMetaService();
        String calculatorTemplate = ruleSystemMetaService.queryCalculatorTemplateByRuleModelId(ruleModelId);

        // 将规则模型模板代码正式渲染成当前规则对应运算代码
        Template template = Engine.use().getTemplateByString(calculatorTemplate);
        HashMap<String, Object> renderInfo = new HashMap<>();
        renderInfo.put("eventParams", eventParams);
        renderInfo.put("combineExpr", combineExpr);
        String calculatorCode = template.renderToString(renderInfo);

        // 4. 正式将规则定义及相关信息写入到mysql中
        ruleSystemMetaService.insertRuleInfo(ruleId, ruleModelId, ruleStr, profileUserBitmap, calculatorCode, 1);


    }

}
