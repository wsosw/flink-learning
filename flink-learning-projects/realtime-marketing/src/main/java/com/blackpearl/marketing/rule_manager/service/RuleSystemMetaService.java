package com.blackpearl.marketing.rule_manager.service;

import com.google.common.io.ByteArrayDataOutput;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.*;

public class RuleSystemMetaService {

    Connection connection;

    public RuleSystemMetaService() throws SQLException {
        connection = DriverManager.getConnection("jdbc:mysql://node01:3306/rtmk?useUnicode=true&characterEncoding=utf8", "root", "123456");
    }


    public String queryCalculatorTemplateByRuleModelId(String ruleModelId) throws SQLException {

        PreparedStatement preparedStatement = connection.prepareStatement(
                "select rule_model_calculator_template from rule_model_info where rule_model_id = ? and rule_model_status = 1");
        preparedStatement.setString(1, ruleModelId);
        ResultSet resultSet = preparedStatement.executeQuery();

        String calculatorTemplate = null;

        while (resultSet.next()) {
            calculatorTemplate = resultSet.getString("rule_model_calculator_template");
        }

        return calculatorTemplate;
    }


    public boolean insertRuleInfo(String ruleId, String ruleModelId, String ruleDefineJson, RoaringBitmap ruleProfileBitmap, String ruleCalculatorCode, int ruleStatus) throws SQLException, IOException {

        // 将bitmap转换成字节数组
        byte[] ruleProfileBitmapBytes = null;
        if (ruleProfileBitmap != null) {
            ByteArrayOutputStream bao = new ByteArrayOutputStream();
            DataOutputStream dao = new DataOutputStream(bao);
            ruleProfileBitmap.serialize(dao);
            ruleProfileBitmapBytes = bao.toByteArray();
        }

        PreparedStatement preparedStatement = connection.prepareStatement(
                "insert into rule_info (rule_id, rule_model_id, rule_define_json, rule_profile_bitmap, rule_calculator_code, rule_status) values (?, ?, ?, ?, ?, ?)");
        preparedStatement.setString(1, ruleId);
        preparedStatement.setString(2, ruleModelId);
        preparedStatement.setString(3, ruleDefineJson);
        preparedStatement.setBytes(4, ruleProfileBitmapBytes);
        preparedStatement.setString(5, ruleCalculatorCode);
        preparedStatement.setInt(6, ruleStatus);

        return preparedStatement.execute();
    }

}
