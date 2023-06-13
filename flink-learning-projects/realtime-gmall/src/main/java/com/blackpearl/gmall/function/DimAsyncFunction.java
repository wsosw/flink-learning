package com.blackpearl.gmall.function;

import com.alibaba.fastjson.JSONObject;
import com.blackpearl.gmall.common.PhoenixConfig;
import com.blackpearl.gmall.utils.DimUtil;
import com.blackpearl.gmall.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.ParseException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 主要功能：异步查询hbase（phoenix）中的数据
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor; // TODO 对这个线程池的理解还有待深入
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(PhoenixConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(PhoenixConfig.PHOENIX_SERVER);

        threadPoolExecutor = ThreadPoolUtil.getThreadPool();
    }

    public abstract String getKey(T input);
    public abstract void join(T input, JSONObject dimInfo) throws ParseException;

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {

                try {
                    // 1. 获取当前数据的主键
                    String key = getKey(input);

                    // 2. 根据根据表名和主键查询对应维度信息
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, key);

                    // 3. 将维度信息补充到宽表中
                    join(input, dimInfo);

                    // 4. 输出数据
                    resultFuture.complete(Collections.singletonList(input));

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut：" + input);
    }
}
