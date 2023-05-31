package com.blackpearl.gmall.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.blackpearl.gmall.bean.TableProcess;
import com.blackpearl.gmall.common.PhoenixConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private OutputTag<JSONObject> jsonObjectOutputTag;
    private MapStateDescriptor<String, TableProcess> tableProcessMapStateDescriptor;
    private Connection connection;

    public TableProcessFunction(OutputTag<JSONObject> jsonObjectOutputTag, MapStateDescriptor<String, TableProcess> tableProcessMapStateDescriptor) {
        this.jsonObjectOutputTag = jsonObjectOutputTag;
        this.tableProcessMapStateDescriptor = tableProcessMapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(PhoenixConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(PhoenixConfig.PHOENIX_SERVER);
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(tableProcessMapStateDescriptor);
        String key = jsonObject.getString("table") + "-" + jsonObject.getString("operation");
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
            // 之前已经过滤掉了delete类型数据，因此可以放心大胆的解析after中的数据
            JSONObject after = jsonObject.getJSONObject("after");

            // 过滤掉不需要的字段
            List<String> fields = Arrays.asList(tableProcess.getSinkColumns().split(","));
//            while (iterator.hasNext()) {
//                Map.Entry<String, Object> next = iterator.next();
//                if (!fields.contains(next.getKey())) iterator.remove();
//            }
            after.entrySet().removeIf(next -> !fields.contains(next.getKey()));

            // 为了方便数据写入到目标系统，在数据中增加sinkTable字段
            jsonObject.put("sinkTable", tableProcess.getSinkTable());

            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) collector.collect(jsonObject);
            else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) readOnlyContext.output(jsonObjectOutputTag, jsonObject);

        } else {
            System.out.println("组合key [" + key + "] 不存在");
        }


    }

    // 广播流数据格式：{"database":"gmall-rtp","table":"table_process","before":"{}","after":"{}","operation":"insert"}
    @Override
    public void processBroadcastElement(String s, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(tableProcessMapStateDescriptor);

        // 获取并解析广播流数据（表配置信息）
        JSONObject jsonObject = JSON.parseObject(s);
        String operation = jsonObject.getString("operation");


        // 如果 operation == delete, 将对应key-value从广播状态中删掉（只删状态，不删表）
        if ("delete".equals(operation)) {
            TableProcess tableProcess = JSON.parseObject(jsonObject.getString("before"), TableProcess.class);
            if (tableProcess.getId() != 0) {
                String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
                broadcastState.remove(key);
            }
        } else {
            TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

            // 判断当前表数据是否写入hbase中，如果是，检查表是否存在，若不存在，则创建表
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                checkTable(tableProcess.getSinkTable(), tableProcess.getSinkColumns(), tableProcess.getSinkPk(), tableProcess.getSinkExtend());
            }

            // 将当前数据写入广播状态中
            String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
            broadcastState.put(key, tableProcess);
        }

    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;

        try {
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            // 组合建表语句
            StringBuffer createTableSQL = new StringBuffer("create table if not exist ")
                    .append(PhoenixConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] fields = sinkColumns.split(",");
            for (int i = 0; i < fields.length; i++) {
                createTableSQL.append(fields[i]).append(" varchar "); // 注意空格
                if (sinkPk.equals(fields[i])) createTableSQL.append("primary key");
                if (i < fields.length - 1) createTableSQL.append(", ");
            }
            createTableSQL.append(")").append(sinkExtend);

            // 打印建表语句
            System.out.println(createTableSQL);

            // SQL预编译
            preparedStatement = connection.prepareStatement(createTableSQL.toString());

            // 执行SQL
            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException("Phoenix表" + sinkTable + "建表失败! ");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}


