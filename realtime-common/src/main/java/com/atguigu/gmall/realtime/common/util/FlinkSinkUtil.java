package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Random;

/**
 * 该工具类适用于需要将数据写入 Kafka 或 Doris 数据库的 Flink 应用程序。
 * 通过提供统一的 Sink 工具类，可以方便地配置和使用不同的 Sink，实现数据的高效传输和存储。
 */
public class FlinkSinkUtil {

    /**
     * 获取 Kafka Sink，用于将字符串数据写入指定的 Kafka 主题
     *
     * @param topic 目标 Kafka 主题
     * @return Kafka Sink
     */
    public static Sink<String> getKafkaSink(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS) // 设置 Kafka 集群地址
                /*
                    这行代码用于配置 KafkaSink 的记录序列化器。
                    KafkaRecordSerializationSchema 是 Flink 提供的一个序列化器，
                    用于将数据转换为 Kafka 消息的格式，并将其发送到指定的 Kafka 主题。
                 */
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(topic) // 设置 Kafka 主题
                        .setValueSerializationSchema(new SimpleStringSchema()) // 设置序列化模式
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE) // 设置投递保证为 EXACTLY_ONCE
                .setTransactionalIdPrefix("atguigu-" + topic + new Random().nextLong()) // 设置事务 ID 前缀
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "") // 设置事务超时时间
                .build();
    }

    /**
     * 获取 Kafka Sink，用于将 Tuple2<JSONObject, TableProcessDwd> 数据写入 Kafka
     *
     * @return Kafka Sink
     */
    public static Sink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink() {
        return KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS) // 设置 Kafka 集群地址
                .setRecordSerializer(new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> dataWithConfig,
                                                                    KafkaSinkContext context,
                                                                    Long timestamp) {
                        String topic = dataWithConfig.f1.getSinkTable(); // 获取目标主题
                        JSONObject data = dataWithConfig.f0;
                        data.remove("op_type"); // 移除 "op_type" 字段
                        return new ProducerRecord<>(topic, data.toJSONString().getBytes(StandardCharsets.UTF_8)); // 创建 ProducerRecord
                    }
                })
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE) // 设置投递保证为 EXACTLY_ONCE
                .setTransactionalIdPrefix("atguigu-" + new Random().nextLong()) // 设置事务 ID 前缀
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "") // 设置事务超时时间
                .build();
    }

    /**
     * 获取 Doris Sink，用于将字符串数据写入 Doris 数据库
     *
     * @param table       目标 Doris 表
     * @param labelPrefix Label 前缀
     * @return Doris Sink
     */
    public static DorisSink<String> getDorisSink(String table, String labelPrefix) {
        Properties props = new Properties();
        props.setProperty("format", "json"); // 设置数据格式为 JSON
        props.setProperty("read_json_by_line", "true"); // 设置每行读取一条 JSON 数据
        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder() // 设置 Doris 连接参数
                        .setFenodes(Constant.DORIS_FE_NODES)
                        .setTableIdentifier(table)
                        .setUsername("root")
                        .setPassword("000000")
                        .build()
                )
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 设置 Doris 执行参数
                        .setLabelPrefix(labelPrefix) // 设置 Stream-Load 的 Label 前缀
                        .disable2PC() // 禁用两阶段提交
                        .setBufferCount(3) // 设置批次条数，默认 3
                        .setBufferSize(1024 * 1024) // 设置批次大小，默认 1M
                        .setCheckInterval(3000) // 设置批次输出间隔，单位毫秒
                        .setMaxRetries(3) // 设置最大重试次数
                        .setStreamLoadProp(props) // 设置 Stream-Load 的数据格式为 JSON
                        .build())
                .setSerializer(new SimpleStringSerializer()) // 设置序列化方式
                .build();
    }
}
