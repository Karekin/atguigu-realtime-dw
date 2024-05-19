package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 该工具类的主要作用是创建并配置一个KafkaSource，用于从Kafka主题中读取数据。具体目标包括：
     * 设置Kafka连接信息：通过 setBootstrapServers 方法，设置Kafka集群的服务器地址，该地址从常量类 Constant 中获取。
     * 配置消费组：通过 setGroupId 方法，设置Kafka的消费组ID。
     * 配置主题：通过 setTopics 方法，指定需要消费的Kafka主题。
     * 设置起始偏移量：通过 setStartingOffsets 方法，设置从最新的偏移量开始消费消息。
     * 配置反序列化器：定义一个自定义的反序列化器 DeserializationSchema<String>，将Kafka消息的字节数组转换为字符串。
         * deserialize 方法实现了字节数组到字符串的转换。
         * isEndOfStream 方法用于指示流是否结束，始终返回 false 表示流未结束。
         * getProducedType 方法返回生成的数据类型信息，这里是字符串类型。
 * 通过调用 build 方法，创建并返回配置好的 KafkaSource 实例。
 * 该工具类常用于Flink流处理应用程序中，需要从Kafka主题中消费数据并进行处理时。
 * 开发者可以通过调用 FlinkSourceUtil.getKafkaSource 方法，轻松获取配置好的KafkaSource，从而专注于数据处理逻辑的实现。
 */
public class FlinkSourceUtil {

    /**
     * 获取KafkaSource实例
     *
     * @param groupId 消费组ID
     * @param topic   主题名称
     * @return KafkaSource<String> 实例
     */
    public static KafkaSource<String> getKafkaSource(String groupId, String topic) {
        // 创建并返回KafkaSource实例
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)  // 设置Kafka的Bootstrap服务器地址
                .setGroupId(groupId)  // 设置消费组ID
                .setTopics(topic)  // 设置主题
                .setStartingOffsets(OffsetsInitializer.latest())  // 设置从最新的偏移量开始消费
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {

                    /**
                     * 反序列化方法，将字节数组转换为字符串
                     *
                     * @param message 字节数组消息
                     * @return 反序列化后的字符串
                     * @throws IOException IO异常
                     */
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message != null) {
                            // 使用UTF-8字符集将字节数组转换为字符串
                            return new String(message, StandardCharsets.UTF_8);
                        }
                        return null;
                    }

                    /**
                     * 判断流是否结束
                     *
                     * @param nextElement 下一个元素
                     * @return 是否结束标志，始终返回false
                     */
                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    /**
                     * 获取生产的类型信息
                     *
                     * @return 类型信息，字符串类型
                     */
                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                })
                .build();  // 构建KafkaSource实例
    }
}
