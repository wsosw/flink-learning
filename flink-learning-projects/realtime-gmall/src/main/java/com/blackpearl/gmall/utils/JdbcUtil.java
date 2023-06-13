package com.blackpearl.gmall.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {

    public static <T> List<T> queryList(Connection connection, String querySQL, Class<T> clz, boolean underScoreToCamel) throws Exception {

        // 存放查询结果
        ArrayList<T> result = new ArrayList<>();

        // 预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(querySQL);

        // 执行查询SQL
        ResultSet resultSet = preparedStatement.executeQuery();

        // 解析resultSet
        ResultSetMetaData metaData = resultSet.getMetaData(); //元信息（列名，列数量等）
        int columnCount = metaData.getColumnCount(); // 列数量
        while (resultSet.next()) {

            T t = clz.newInstance(); // 创建泛型对象

            // 给泛型对象赋值
            for (int i = 1; i <= columnCount; i++) {

                String columnName = metaData.getColumnName(i);// getColumnName(i) -> 下标从1开始

                // mysql中一般以小写下划线命名字段，hbase（phoenix）中一般以大写下划线命名字段，java变量一般为小驼峰
                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }

                // 获取列值
                Object value = resultSet.getObject(i);

                // 给泛型对象赋值
                BeanUtils.setProperty(t, columnName, value);
            }

            // 将每个对象添加只结果集
            result.add(t);
        }

        preparedStatement.close();
        resultSet.close();

        return result;
    }


}
