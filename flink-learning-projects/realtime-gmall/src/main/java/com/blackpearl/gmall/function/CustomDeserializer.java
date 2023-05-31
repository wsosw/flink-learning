package com.blackpearl.gmall.function;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class CustomDeserializer implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        // 获取主题信息：库名、表名
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        String dbName = split[1];
        String tableName = split[2];

        // 获取操作类型: READ DELETE CREATE UPDATE TRUNCATE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

        Struct value = (Struct) sourceRecord.value();
        // 获取变化前数据
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) { // 要注意判空
            for (Field field : before.schema().fields()) {
                Object o = before.get(field);
                beforeJson.put(field.name(), o);
            }
        }

        // 获取变化后数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) { // 要注意判空
            for (Field field : after.schema().fields()) {
                Object o = after.get(field);
                afterJson.put(field.name(), o);
            }
        }

        // 创建自定义JSON对象
        JSONObject result = new JSONObject();
        result.put("database", dbName);
        result.put("table", tableName);
        result.put("operation", operation);
        result.put("before", beforeJson);
        result.put("after", afterJson);

        // 发送数据
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
