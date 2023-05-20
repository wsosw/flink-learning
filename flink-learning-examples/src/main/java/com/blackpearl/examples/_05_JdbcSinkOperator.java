package com.blackpearl.examples;

import com.alibaba.fastjson.JSON;
import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class _05_JdbcSinkOperator {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///f:/flink/ckpt");

        DataStreamSource<EventLog> source = env.addSource(new MySourceFunction());

        // AT_LEAST_ONCE Jdbc Sink
        SinkFunction<EventLog> jdbcSink1 = JdbcSink.sink(
                "insert into event_log values (?, ?, ?, ?, ?) on duplicate key update sessionId=?,eventId=?,ts=?,eventInfo=?",
                new JdbcStatementBuilder<EventLog>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, EventLog eventLog) throws SQLException {

                        // 替换占位符
                        preparedStatement.setLong(1, eventLog.getGuid());
                        preparedStatement.setString(2, eventLog.getSessionId());
                        preparedStatement.setString(3, eventLog.getEventId());
                        preparedStatement.setLong(4, eventLog.getTimeStamp());
                        preparedStatement.setString(5, JSON.toJSONString(eventLog.getEventInfo()));

                        preparedStatement.setString(6, eventLog.getSessionId());
                        preparedStatement.setString(7, eventLog.getEventId());
                        preparedStatement.setLong(8, eventLog.getTimeStamp());
                        preparedStatement.setString(9, JSON.toJSONString(eventLog.getEventInfo()));

                    }
                },
                JdbcExecutionOptions.builder() // 执行时的参数配置：最大重试次数，按时间间隔提交，按批量提交等
                        .withMaxRetries(3)  // 最大重试次数为3
                        /*.withBatchIntervalMs(1000)*/ // 按时间间隔提交，多长时间提交一次
                        .withBatchSize(1) // 按批量提交，攒多少条数据提交一次
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder() // jdbc连接器参数
                        .withUrl("jdbc:mysql://node01:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );
        /*source.addSink(jdbcSink1);*/



        /**
         * 问题：
         * Caused by: org.apache.flink.util.FlinkRuntimeException: unable to recover, error -3: resource manager error has occurred. [XAER_RMERR: Fatal error occurred in the transaction branch - check your data for consistency]
         *     at org.apache.flink.connector.jdbc.xa.XaFacadeImpl.wrapException(XaFacadeImpl.java:369)
         *     at org.apache.flink.connector.jdbc.xa.XaFacadeImpl.lambda$execute$12(XaFacadeImpl.java:280)
         * 原因：
         * flink-jdbc-eos需要支持分布式事务XA, 详情见：
         *     https://zhuanlan.zhihu.com/p/400930855
         *     https://blog.csdn.net/java_creatMylief/article/details/128715459
         * 解决办法：
         * 授予root,XA_RECOVER_ADMIN权限即可
         *     GRANT XA_RECOVER_ADMIN ON *.* TO 'root'@'%';
         *     FLUSH PRIVILEGES;
         */

        // EXACTLY_ONCE Jdbc Sink
        SinkFunction<EventLog> jdbcSink2 = JdbcSink.exactlyOnceSink(
                "insert into event_log values (?, ?, ?, ?, ?) on duplicate key update sessionId=?,eventId=?,ts=?,eventInfo=?",
                new JdbcStatementBuilder<EventLog>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, EventLog eventLog) throws SQLException {

                        // 替换占位符
                        preparedStatement.setLong(1, eventLog.getGuid());
                        preparedStatement.setString(2, eventLog.getSessionId());
                        preparedStatement.setString(3, eventLog.getEventId());
                        preparedStatement.setLong(4, eventLog.getTimeStamp());
                        preparedStatement.setString(5, JSON.toJSONString(eventLog.getEventInfo()));

                        preparedStatement.setString(6, eventLog.getSessionId());
                        preparedStatement.setString(7, eventLog.getEventId());
                        preparedStatement.setLong(8, eventLog.getTimeStamp());
                        preparedStatement.setString(9, JSON.toJSONString(eventLog.getEventInfo()));
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(1)
                        .build(),
                JdbcExactlyOnceOptions.builder()
                        .withTransactionPerConnection(true) // mysql不支持同一个连接上存在并行的多个事务，必须把该参数设置为true
                        .build(),
                new SerializableSupplier<XADataSource>() {
                    @Override
                    public XADataSource get() {

                        // 这个其实就是EOS下jdbc连接器参数配置的封装
                        MysqlXADataSource xaDataSource = new MysqlXADataSource();
                        xaDataSource.setUrl("jdbc:mysql://node01:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8");
                        xaDataSource.setUser("root");
                        xaDataSource.setPassword("123456");

                        return xaDataSource;
                    }
                }
        );
        source.addSink(jdbcSink2);


        env.execute("jdbc-sink-operator");

    }


}
