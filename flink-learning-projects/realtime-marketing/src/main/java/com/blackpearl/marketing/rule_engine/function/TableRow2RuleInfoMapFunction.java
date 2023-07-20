package com.blackpearl.marketing.rule_engine.function;

import com.blackpearl.marketing.common.pojo.RuleInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;

public class TableRow2RuleInfoMapFunction implements MapFunction<Row, RuleInfo> {
    @Override
    public RuleInfo map(Row row) throws Exception {

        RuleInfo ruleInfo = new RuleInfo();

        if (row.getKind() == RowKind.DELETE) {

            ruleInfo.setOperationType("DELETE");
            ruleInfo.setId(row.getFieldAs("id"));
            ruleInfo.setRuleId(row.getFieldAs("rule_id"));

        } else if (row.getKind() == RowKind.UPDATE_AFTER) {

            ruleInfo.setOperationType("UPDATE");
            setAttributes(ruleInfo, row);

        } else if (row.getKind() == RowKind.INSERT) {

            ruleInfo.setOperationType("INSERT");
            setAttributes(ruleInfo, row);

        }

        // 别忘了在使用时要过滤 UPDATE_BEFORE 的数据，这类数据的 id == 0

        return ruleInfo;
    }


    private void setAttributes(RuleInfo ruleInfo, Row row) {

        int id = row.getFieldAs("id");
        String ruleId = row.getFieldAs("rule_id");
        String ruleModelId = row.getFieldAs("rule_model_id");
        String ruleDefinitionJson = row.getFieldAs("rule_define_json");
        byte[] bitmapBytes = row.getFieldAs("rule_profile_bitmap");
        String ruleCalculatorCode = row.getFieldAs("rule_calculator_code");
        int ruleStatus = row.getFieldAs("rule_status");
        String ruleDesc = row.getFieldAs("rule_desc");

        // 序列化画像人群bitmap
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf();
        if (bitmapBytes != null) {
            bitmap.serialize(ByteBuffer.wrap(bitmapBytes));
        }

        ruleInfo.setId(id);
        ruleInfo.setRuleModelId(ruleId);
        ruleInfo.setRuleModelId(ruleModelId);
        ruleInfo.setRuleDefinitionJson(ruleDefinitionJson);
        ruleInfo.setProfileUserBitmap(bitmap);
        ruleInfo.setRuleCalculatorCode(ruleCalculatorCode);
        ruleInfo.setRuleStatus(ruleStatus);
        ruleInfo.setRuleDesc(ruleDesc);
    }


}
