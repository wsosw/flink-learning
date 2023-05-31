package com.blackpearl.gmall.common;

public class PhoenixConfig {

    public static final String HBASE_SCHEMA = "GMALL_REALTIME";
    //Phoenix 驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    //Phoenix 连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:node01,node02,node03:2181";

}
