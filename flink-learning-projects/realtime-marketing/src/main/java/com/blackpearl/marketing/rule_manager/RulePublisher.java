package com.blackpearl.marketing.rule_manager;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.blackpearl.marketing.common.pojo.ActionSequenceParam;
import com.blackpearl.marketing.common.pojo.AttributeParam;
import com.blackpearl.marketing.common.pojo.EventParam;
import com.blackpearl.marketing.rule_manager.service.ActionConditionQueryService;
import com.blackpearl.marketing.rule_manager.service.ProfileConditionQueryService;
import com.blackpearl.marketing.rule_manager.service.RuleSystemMetaService;
import com.jfinal.template.Engine;
import com.jfinal.template.Template;
import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
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

@Slf4j
public class RulePublisher {

    public static void main(String[] args) throws SQLException, IOException {

        // 规则模型一包含的条件：画像条件，行为次数条件，[触发事件]
        //publishRuleModel1();

        // 规则模型二包含的条件：画像条件，行为次数条件，行为序列条件，[触发事件]
        publishRuleModel2();

        // 规则模型三包含的条件：触发事件，[定时器]
        //publishRuleModel3();

    }


    /**
     * 规则模型一：画像条件，行为次数条件
     */
    private static void publishRuleModel1() throws IOException, SQLException {

        // 准备条件：从文件读规则定义json数据（生产中由前端传入）
        String ruleStr = new String(Files.readAllBytes(Paths.get("D:\\WorkSpace\\Java\\flink-learning\\flink-learning-projects\\realtime-marketing\\src\\main\\java\\com\\blackpearl\\marketing\\rule_model\\definition\\rule1.json")));
        JSONObject ruleDefinition = JSON.parseObject(ruleStr);

        // TODO 目前默认画像条件和行为次数条件都是有值的，后期如果要灵活处理，需要将对应字段做判空容错，并修改对应规则运算机代码（groovy）
        // 准备条件：取出规则发布和历史状态计算需要的相关参数字段
        String ruleId = ruleDefinition.getString("ruleId");
        String ruleModelId = ruleDefinition.getString("ruleModelId");
        JSONArray profileCondition = ruleDefinition.getJSONArray("profileCondition");
        JSONObject actionCountCondition = ruleDefinition.getJSONObject("actionCountCondition");
        JSONArray eventParams = actionCountCondition.getJSONArray("eventParams");
        String combineExpr = actionCountCondition.getString("combineExpr");

        // 准备条件：创建需要的service实例
        ProfileConditionQueryService profileConditionQueryService = new ProfileConditionQueryService();
        ActionConditionQueryService actionConditionQueryService = new ActionConditionQueryService();
        RuleSystemMetaService ruleSystemMetaService = new RuleSystemMetaService();

        log.info("规则发布准备工作已完成，具体计算任务开始...");

        // 人群圈选：取出用户画像条件，在ES中查询与画像条件匹配的用户，构造受众人群Bitmap
        RoaringBitmap profileUserBitmap = profileConditionQueryService.queryProfileUsers(profileCondition);

        // ES掌握的还不行，有时候会出问题，以下这行直接生成bitmap的代码仅在测试中使用
        // RoaringBitmap profileUserBitmap = RoaringBitmap.bitmapOf(1, 2, 3);

        log.info("人群圈选计算完成，已得到符合画像条件的受众人群Bitmap...");

        // 行为次数条件计算：取出行为次数条件，在doris中计算历史结果，写入到redis状态机中（为flink计算提供初始值）
        for (int i = 0; i < eventParams.size(); i++) {
            actionConditionQueryService.processActionCountCondition(eventParams.getJSONObject(i), ruleId, profileUserBitmap);
        }

        log.info("用户历史行为次数统计完成，已将历史状态写入状态机Redis...");

        // 查询当前规则对应模型的运算机模板代码，并将规则模型模板代码正式渲染成当前规则对应运算代码
        String calculatorTemplate = ruleSystemMetaService.queryCalculatorTemplateByRuleModelId(ruleModelId);
        Template template = Engine.use().getTemplateByString(calculatorTemplate);
        HashMap<String, Object> renderInfo = new HashMap<>();
        renderInfo.put("eventParams", eventParams);
        renderInfo.put("combineExpr", combineExpr);
        String calculatorCode = template.renderToString(renderInfo);

        log.info("当前规则模板代码查询完成，已将模板代码渲染成当前规则具体代码...");

        // 正式将规则定义及相关信息写入到mysql中
        ruleSystemMetaService.insertRuleInfo(ruleId, ruleModelId, ruleStr, profileUserBitmap, calculatorCode, 1);

        log.info("所有规则元数据计算完毕，已将规则元数据发布到Mysql...");
    }

    /**
     * 规则模型二：画像条件，行为次数条件，行为序列条件，[触发事件]
     */
    private static void publishRuleModel2() throws IOException, SQLException {

        // 准备条件：从文件读规则定义json数据（生产中由前端传入）
        String ruleStr = new String(Files.readAllBytes(Paths.get("D:\\WorkSpace\\Java\\flink-learning\\flink-learning-projects\\realtime-marketing\\src\\main\\java\\com\\blackpearl\\marketing\\rule_model\\definition\\rule2.json")));
        JSONObject ruleDefinition = JSON.parseObject(ruleStr);

        // TODO 目前默认画像条件，行为次数条件和行为序列条件都是有值的，后期如果要灵活处理，需要将对应字段做判空容错，并修改对应规则运算机代码（groovy）
        // 准备条件：取出规则发布和历史状态计算需要的相关参数字段
        String ruleId = ruleDefinition.getString("ruleId");
        String ruleModelId = ruleDefinition.getString("ruleModelId");
        JSONArray profileCondition = ruleDefinition.getJSONArray("profileCondition");
        JSONObject actionCountCondition = ruleDefinition.getJSONObject("actionCountCondition");
        JSONArray actionCountEventParams = actionCountCondition.getJSONArray("eventParams");
        String actionCountCombineExpr = actionCountCondition.getString("combineExpr");
        JSONObject actionSequenceCondition = ruleDefinition.getJSONObject("actionSequenceCondition");
        JSONArray actionSequenceEventParams = actionSequenceCondition.getJSONArray("eventParams");
        String ruleCombineExpr = ruleDefinition.getString("combineExpr");


        // 准备条件：创建需要的service实例
        ProfileConditionQueryService profileConditionQueryService = new ProfileConditionQueryService();
        ActionConditionQueryService actionConditionQueryService = new ActionConditionQueryService();
        RuleSystemMetaService ruleSystemMetaService = new RuleSystemMetaService();

        log.info("规则发布准备工作已完成，具体计算任务开始...");

        // 人群圈选：取出用户画像条件，在ES中查询与画像条件匹配的用户，构造受众人群Bitmap
        RoaringBitmap profileUserBitmap = profileConditionQueryService.queryProfileUsers(profileCondition);

        // ES掌握的还不行，有时候会出问题，以下这行直接生成bitmap的代码仅在测试中使用
        // RoaringBitmap profileUserBitmap = RoaringBitmap.bitmapOf(1, 2, 3);

        log.info("人群圈选计算完成，已得到符合画像条件的受众人群Bitmap...");

        // 行为次数条件历史值计算：取出行为次数条件，在doris中计算历史结果，写入到redis状态机中（为flink计算提供初始值）
        for (int i = 0; i < actionCountEventParams.size(); i++) {
            actionConditionQueryService.processActionCountCondition(actionCountEventParams.getJSONObject(i), ruleId, profileUserBitmap);
        }

        log.info("用户行为次数历史值统计完成，已将历史状态写入Redis状态机...");

        // 行为序列条件历史值处理
        ArrayList<EventParam> eventParamList = new ArrayList<>();
        for (int i = 0; i < actionSequenceEventParams.size(); i++) {
            JSONObject actionSequenceEventParam = actionSequenceEventParams.getJSONObject(i);
            String eventId = actionSequenceEventParam.getString("eventId");
            JSONArray attributeParams = actionSequenceEventParam.getJSONArray("attributeParams");

            ArrayList<AttributeParam> attributeParamList = new ArrayList<>();
            for (int j = 0; j < attributeParams.size(); j++) {
                JSONObject attributeParam = attributeParams.getJSONObject(i);
                String attributeName = attributeParam.getString("attributeName");
                String compareType = attributeParam.getString("compareType");
                String compareValue = attributeParam.getString("compareValue");
                attributeParamList.add(new AttributeParam(attributeName, compareType, compareValue));
            }
            eventParamList.add(new EventParam(eventId, attributeParamList));
        }
        String windowStart = actionSequenceCondition.getString("windowStart");
        String windowEnd = actionSequenceCondition.getString("windowEnd");
        Integer conditionId = actionSequenceCondition.getInteger("conditionId");
        Integer sequenceCount = actionSequenceCondition.getInteger("sequenceCount");
        ActionSequenceParam actionSequenceParam = new ActionSequenceParam(windowStart, windowEnd, conditionId, sequenceCount, eventParamList);
        actionConditionQueryService.processActionSequenceCondition(actionSequenceParam, ruleId, profileUserBitmap);

        log.info("用户行为序列历史值统计完成，已将历史状态写入Redis状态机...");


        // 查询当前规则对应模型的运算机模板代码，并将规则模型模板代码正式渲染成当前规则对应运算代码
        String calculatorTemplate = ruleSystemMetaService.queryCalculatorTemplateByRuleModelId(ruleModelId);
        Template template = Engine.use().getTemplateByString(calculatorTemplate);
        HashMap<String, Object> renderInfo = new HashMap<>();
        renderInfo.put("actionCountEventParams", actionCountEventParams);
        renderInfo.put("actionCountCombineExpr", actionCountCombineExpr);
        renderInfo.put("ruleCombineExpr", ruleCombineExpr);
        String calculatorCode = template.renderToString(renderInfo);

        log.info("当前规则模板代码查询完成，已将模板代码渲染成当前规则具体代码...");

        // 正式将规则定义及相关信息写入到mysql中
        ruleSystemMetaService.insertRuleInfo(ruleId, ruleModelId, ruleStr, profileUserBitmap, calculatorCode, 1);

        log.info("所有规则元数据计算完毕，已将规则元数据发布到Mysql...");
    }

    /**
     * 规则模型三：触发事件，[定时器]
     */
    private static void publishRuleModel3() {

    }

}
