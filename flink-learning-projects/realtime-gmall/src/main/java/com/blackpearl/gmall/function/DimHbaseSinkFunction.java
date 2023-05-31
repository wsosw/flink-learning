package com.blackpearl.gmall.function;

import com.alibaba.fastjson.JSONObject;
import com.blackpearl.gmall.common.PhoenixConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DimHbaseSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(PhoenixConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(PhoenixConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }


    // 数据格式：
    // {"sinkTable": "", "database": "", "table": "", "operation": "", "before"： "{}"， "after"： "{}"}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        PreparedStatement preparedStatement = null;
        try {
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");

            // 构建sql语句
            String upsertKeys = StringUtils.join(after.keySet(), ",");
            String upsertValues = StringUtils.join(after.values(), ",");
            String upsertSQL = String.format("upsert into %s.%s (%s) values (%s)", PhoenixConfig.HBASE_SCHEMA, sinkTable, upsertKeys, upsertValues);
            System.out.println(upsertSQL);

            preparedStatement = connection.prepareStatement(upsertSQL);

            // TODO 如果当前数据为更新操作（value.operation == update），则先删除redis中的数据

            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) preparedStatement.close();
        }

    }


}
