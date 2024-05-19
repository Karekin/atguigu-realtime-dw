package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;

/**
 * 该工具类 SQLUtil 提供了三个静态方法，用于生成Kafka连接器的DDL语句。
 * 这些DDL语句用于在Flink SQL中创建Kafka源表和目标表。具体目标包括：
     * 生成Kafka源表的DDL语句：方法 getKafkaDDLSource 生成用于从Kafka主题中读取数据的DDL语句。
        * 该语句指定了Kafka连接器、消费组ID、主题、服务器地址、启动模式和数据格式。
     * 生成Kafka目标表的DDL语句：方法 getKafkaDDLSink 生成用于向Kafka主题写入数据的DDL语句。
        * 该语句指定了Kafka连接器、主题、服务器地址和数据格式。
     * 生成Upsert Kafka表的DDL语句：方法 getUpsertKafkaDDL 生成用于向Kafka主题进行Upsert操作的DDL语句。
        * Upsert Kafka连接器支持插入和更新操作。该语句指定了连接器类型、主题、服务器地址、键和值的JSON解析错误处理方式，以及数据格式。

 * 使用场景：这些方法适用于需要在Flink SQL中创建Kafka源表或目标表的场景。
 * 通过调用这些方法，开发者可以生成相应的DDL语句，并在Flink SQL中执行这些语句，
 * 以便从Kafka读取数据或向Kafka写入数据。这样可以简化DDL语句的编写，并确保一致性和可维护性。
 */
public class SQLUtil {

    /**
     * 生成Kafka源表的DDL语句
     *
     * @param groupId 消费组ID
     * @param topic   主题名称
     * @return Kafka源表的DDL语句
     */
    public static String getKafkaDDLSource(String groupId, String topic) {
        return "with(" +
                "  'connector' = 'kafka'," +  // 指定连接器类型为Kafka
                "  'properties.group.id' = '" + groupId + "'," +  // 设置Kafka消费组ID
                "  'topic' = '" + topic + "'," +  // 指定Kafka主题
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +  // 设置Kafka服务器地址
                "  'scan.startup.mode' = 'latest-offset'," +  // 设置启动模式为从最新的偏移量开始
                "  'json.ignore-parse-errors' = 'true'," +  // 忽略JSON解析错误
                "  'format' = 'json' " +  // 设置数据格式为JSON
                ")";
    }

    /**
     * 生成Kafka目标表的DDL语句
     *
     * @param topic 主题名称
     * @return Kafka目标表的DDL语句
     */
    public static String getKafkaDDLSink(String topic) {
        return "with(" +
                "  'connector' = 'kafka'," +  // 指定连接器类型为Kafka
                "  'topic' = '" + topic + "'," +  // 指定Kafka主题
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +  // 设置Kafka服务器地址
                "  'format' = 'json' " +  // 设置数据格式为JSON
                ")";
    }

    /**
     * 生成Upsert Kafka表的DDL语句
     *
     * @param topic 主题名称
     * @return Upsert Kafka表的DDL语句
     */
    public static String getUpsertKafkaDDL(String topic) {
        return "with(" +
                "  'connector' = 'upsert-kafka'," +  // 指定连接器类型为Upsert Kafka
                "  'topic' = '" + topic + "'," +  // 指定Kafka主题
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +  // 设置Kafka服务器地址
                "  'key.json.ignore-parse-errors' = 'true'," +  // 忽略键的JSON解析错误
                "  'value.json.ignore-parse-errors' = 'true'," +  // 忽略值的JSON解析错误
                "  'key.format' = 'json', " +  // 设置键的数据格式为JSON
                "  'value.format' = 'json' " +  // 设置值的数据格式为JSON
                ")";
    }
}
