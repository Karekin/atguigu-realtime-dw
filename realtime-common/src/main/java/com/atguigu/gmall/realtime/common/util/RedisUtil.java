package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 该代码定义了一个 RedisUtil 工具类，用于与Redis进行同步和异步交互。具体目标包括：
     * 连接池管理：通过Jedis连接池管理Redis连接，提供高效的连接复用。
     * 同步操作：提供获取连接、读写数据和关闭连接的方法。
     * 异步操作：使用Lettuce库提供异步的读写数据方法，支持高并发场景。

 * 该工具类适用于需要与Redis进行高效数据交互的Flink应用程序。
 * 通过使用连接池管理连接，提供同步和异步的读写操作，可以在高并发和分布式环境中实现高效的数据存储和检索。
 */
public class RedisUtil {

    // 定义Jedis连接池
    private final static JedisPool pool;

    // 静态代码块，初始化Jedis连接池
    static {
        GenericObjectPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(300); // 最大连接数
        config.setMaxIdle(10);   // 最大空闲连接数
        config.setMinIdle(2);    // 最小空闲连接数

        config.setTestOnCreate(true);  // 创建连接时进行测试
        config.setTestOnBorrow(true);  // 获取连接时进行测试
        config.setTestOnReturn(true);  // 返回连接时进行测试

        config.setMaxWaitMillis(10 * 1000); // 最大等待时间，单位毫秒

        pool = new JedisPool(config, "hadoop102", 6379); // 初始化连接池
    }

    /**
     * 获取Jedis实例
     *
     * @return Jedis实例
     */
    public static Jedis getJedis() {
        Jedis jedis = pool.getResource(); // 从连接池获取资源
        jedis.select(4); // 选择4号库
        return jedis;
    }

    /**
     * 从Redis读取维度数据
     *
     * @param jedis     Jedis对象
     * @param tableName 表名
     * @param id        维度的ID值
     * @return 这条维度组成的JSONObject对象
     */
    public static JSONObject readDim(Jedis jedis, String tableName, String id) {
        String key = getKey(tableName, id);
        String jsonStr = jedis.get(key);
        if (jsonStr != null) {
            return JSON.parseObject(jsonStr);
        }
        return null;
    }

    /**
     * 将维度数据写入到Redis
     *
     * @param jedis     Jedis对象
     * @param tableName 表名
     * @param id        维度的ID值
     * @param dim       要写入的维度数据
     */
    public static void writeDim(Jedis jedis, String tableName, String id, JSONObject dim) {
        jedis.setex(getKey(tableName, id), Constant.TWO_DAY_SECONDS, dim.toJSONString());
    }

    /**
     * 获取Redis键的格式化字符串
     *
     * @param tableName 表名
     * @param id        维度的ID值
     * @return Redis键
     */
    public static String getKey(String tableName, String id) {
        return tableName + ":" + id;
    }

    /**
     * 关闭Jedis实例
     *
     * @param jedis Jedis实例
     */
    public static void closeJedis(Jedis jedis) {
        if (jedis != null) {
            jedis.close(); // 关闭连接
        }
    }

    /**
     * 获取Redis异步连接
     *
     * @return 异步连接对象
     */
    public static StatefulRedisConnection<String, String> getRedisAsyncConnection() {
        RedisClient redisClient = RedisClient.create("redis://hadoop102:6379/2");
        return redisClient.connect();
    }

    /**
     * 关闭Redis异步连接
     *
     * @param redisAsyncConn 异步连接对象
     */
    public static void closeRedisAsyncConnection(StatefulRedisConnection<String, String> redisAsyncConn) {
        if (redisAsyncConn != null) {
            redisAsyncConn.close();
        }
    }

    /**
     * 异步读取Redis中的维度数据
     *
     * @param redisAsyncConn 异步连接对象
     * @param tableName      表名
     * @param id             维度的ID值
     * @return 读取到的维度数据，封装为JSONObject对象
     */
    public static JSONObject readDimAsync(StatefulRedisConnection<String, String> redisAsyncConn,
                                          String tableName,
                                          String id) {
        RedisAsyncCommands<String, String> asyncCommand = redisAsyncConn.async();
        String key = getKey(tableName, id);
        try {
            String json = asyncCommand.get(key).get(); // 异步获取数据
            if (json != null) {
                return JSON.parseObject(json);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * 异步将维度数据写入Redis
     *
     * @param redisAsyncConn 异步连接对象
     * @param tableName      表名
     * @param id             维度的ID值
     * @param dim            要写入的维度数据
     */
    public static void writeDimAsync(StatefulRedisConnection<String, String> redisAsyncConn,
                                     String tableName,
                                     String id,
                                     JSONObject dim) {
        RedisAsyncCommands<String, String> asyncCommand = redisAsyncConn.async();
        String key = getKey(tableName, id);
        asyncCommand.setex(key, Constant.TWO_DAY_SECONDS, dim.toJSONString()); // 异步写入数据并设置过期时间
    }
}
