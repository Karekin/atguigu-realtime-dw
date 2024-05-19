package com.atguigu.gmall.realtime.common.base;

import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * 该代码定义了一个Flink应用程序的基础框架，主要作用包括：
     * 设置环境：配置Flink流执行环境，包括Hadoop用户、WebUI端口和并行度。
     * 配置状态后端和检查点：设置状态后端为 HashMapStateBackend，并启用检查点机制，以保证数据处理的可靠性和一致性。
     * 读取Kafka数据：通过 FlinkSourceUtil 从Kafka指定主题中读取数据，并将其封装为Flink数据流。
     * 处理逻辑：调用抽象方法 handle，由子类实现具体的数据处理逻辑。
     * 执行Flink作业：最终启动Flink作业，执行数据流的处理。
 * 通过这种方式， BaseApp 提供了一个通用的框架，子类只需实现 handle 方法，定义具体的处理逻辑即可，简化了Flink应用程序的开发。
 */
public abstract class BaseApp {
    /**
     * 抽象方法，供子类实现具体的处理逻辑
     *
     * @param env    Flink流执行环境
     * @param stream 数据流源
     */
    public abstract void handle(StreamExecutionEnvironment env,
                                DataStreamSource<String> stream);

    /**
     * 启动Flink应用程序
     *
     * @param port        WebUI绑定的端口
     * @param parallelism 并行度
     * @param ckAndGroupId 检查点和消费组ID
     * @param topic       Kafka主题
     */
    public void start(int port, int parallelism, String ckAndGroupId, String topic) {
        // 1. 环境准备
        // 1.1 设置操作Hadoop的用户名为Hadoop超级用户atguigu
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // 1.2 获取流处理环境，并指定本地测试时启动WebUI所绑定的端口
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 1.3 设置并行度
        env.setParallelism(parallelism);

        // 1.4 状态后端及检查点相关配置
        // 1.4.1 设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 1.4.2 开启checkpoint，每5000毫秒触发一次
        env.enableCheckpointing(5000);

        // 1.4.3 设置checkpoint模式: 精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 1.4.4 设置checkpoint存储路径
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall2023/stream/" + ckAndGroupId);

        // 1.4.5 设置checkpoint并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 1.4.6 设置checkpoint之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // 1.4.7 设置checkpoint的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);

        // 1.4.8 设置job取消时checkpoint的保留策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        // 1.5 从Kafka目标主题读取数据，封装为流
        KafkaSource<String> source = FlinkSourceUtil.getKafkaSource(ckAndGroupId, topic);
        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka_source");

        // 2. 执行具体的处理逻辑
        handle(env, stream);

        // 3. 执行Job
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
