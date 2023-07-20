package com.blackpearl.marketing.rule_engine.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 封装规则触达结果的javabean
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleMatchResult {
    private int guid;
    private String ruleId;
    private long matchTime;
}
