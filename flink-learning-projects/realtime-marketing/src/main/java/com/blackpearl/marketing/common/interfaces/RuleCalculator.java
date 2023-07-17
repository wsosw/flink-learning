package com.blackpearl.marketing.common.interfaces;

import com.alibaba.fastjson.JSONObject;
import com.blackpearl.marketing.common.pojo.EventLog;
import org.roaringbitmap.RoaringBitmap;

import java.text.ParseException;

public interface RuleCalculator {


    // 规则运算机初始化方法
    void init(JSONObject ruleDefinition, RoaringBitmap profileUserBitmap);

    // 规则条件运算逻辑
    void calculate(EventLog eventLog) throws ParseException;

    // 判断指定用户是否已经满足当前规则的所有条件
    boolean match(int guid);




}
