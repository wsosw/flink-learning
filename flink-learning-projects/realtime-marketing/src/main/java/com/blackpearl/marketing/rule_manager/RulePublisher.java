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

/**
 *
 * 主要考虑：到底是给所有人做运算，还是给画像人群做运算？
 *   搞清楚了，不需要考虑这么麻烦！！！
 *     -> 如果规则中不存在画像条件，则需要计算所有人的历史状态。
 *     -> 如果规则中存在画像条件，那在计算圈选人群后，就只将受众人群的历史状态写入到redis状态机中。即使画像数据更新了，可能有一些用户也符合画像条件了，
 *      但是在更新之前他是不属于圈选人群的，因此历史数据不生效。这部分人的状态记录，直接在更新后由flink调用运算机实时计算写入。
 *
 * 规则管理平台仍需考虑的问题：（X）
 *   1. 更新规则后，规则的条件可能会发生变化，此时redis中的状态数据该怎样处理呢？
 *     -> 最简单也可以说最暴力的方法，其实就是按删除和新增两步来处理
 *     -> 直接删除掉该规则在redis中存储的所有状态，然后在重新查询doris历史数据，重新把新状态写入redis。这样可以少做很多判断
 *   2. 有些规则上线后，可能直接在doris中的历史数据中就已经满足了所有条件，这时应该怎样去触发呢？
 *
 * 注意：
 *   规则管理平台使用doris数据，flink计算引擎使用kafka数据。两个程序之间共同使用redis保存状态信息，不涉及其他任何交互
 *     -> 以上问题实际上只设计到doris中的历史数据，要保证这些问题的处理和flink计算引擎解耦。所有计算不要不牵扯到flink计算引擎
 *
 */

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

        // TODO 为使规则匹配更加灵活，后期要对相关条件字段做容错处理（判断画像条件，行为次数条件，行为序列条件等是否都存在），目前默认所有字段都有值。

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
