package com.blackpearl.gmall.utils;

import com.alibaba.fastjson.JSONObject;
import com.blackpearl.gmall.common.PhoenixConfig;
import com.sun.media.sound.SoftVoice;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

public class DimUtil {

    /**
     * 查询hbase-dim层的维表数据
     *
     * @param connection
     * @param tableName
     * @param key
     * @return
     */
    public static JSONObject getDimInfo(Connection connection, String tableName, String key) throws Exception {

        // 为提高维度数据查询速度，dim层数据使用redis做独立缓存
        Jedis jedis = RedisUtil.getJedis();

        // 在redis中直接存String类型数据而不是使用（map，set等），是因为数据量过大时，不至于把数据集中在一台节点，使一台节点压力过大
        // redis key 类型格式：DIM:TABLENAME:KEY => 示例：DIM:DIM_USER_INFO:97
        String redisKey = "DIM:" + tableName + ":" + key;
        String redisValue = jedis.get(redisKey);
        if (redisValue != null) {
            jedis.expire(redisKey, 24 * 60 * 60); // 重置过期时间为24小时
            jedis.close();
            return JSONObject.parseObject(redisValue);
        }

        // 如果redis中没有要查询的维度数据，需要在hbase（phoenix）中查询
        String querySQL = String.format("select * from %s.%s where id = '%s'", PhoenixConfig.HBASE_SCHEMA, tableName, key);
        List<JSONObject> list = JdbcUtil.queryList(connection, querySQL, JSONObject.class, false);

        JSONObject redisValueJson = null;
        if (list.size() > 0) {
            redisValueJson = list.get(0);

            // 将查询到的结果写入到redis
            jedis.set(redisKey, redisValueJson.toJSONString());
            jedis.expire(redisKey,24 * 60 * 60);
            jedis.close();
        }

        return redisValueJson;
    }

    /**
     * 删除redis中的缓存数据
     * @param tableName
     * @param key
     */
    public static void delRedisDimInfo(String tableName, String key) {
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + key;
        jedis.del(redisKey);
        jedis.close();
    }


}
