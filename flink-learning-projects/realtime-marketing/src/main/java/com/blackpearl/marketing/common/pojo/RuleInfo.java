package com.blackpearl.marketing.common.pojo;

import com.blackpearl.marketing.common.interfaces.RuleCalculator;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.roaringbitmap.RoaringBitmap;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleInfo {
    private int id;
    private String operationType;
    private String ruleId;
    private String ruleModelId;
    private String ruleDefinitionJson;
    private RoaringBitmap profileUserBitmap;
    private String ruleCalculatorCode;
    private int ruleStatus;
    private String ruleDesc;
    private RuleCalculator ruleCalculator; //反射出来的运算机类的实例
}
