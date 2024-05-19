package com.atguigu.gmall.realtime.common.base;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * 该代码定义了一个Flink SQL应用程序的基础框架，主要作用包括：
     * 设置环境：配置Flink流执行环境，包括Hadoop用户、WebUI端口和并行度。
     * 配置状态后端和检查点：设置状态后端为 HashMapStateBackend，并启用检查点机制，以保证数据处理的可靠性和一致性。
     * 创建表执行环境：通过 StreamTableEnvironment.create 创建Flink表执行环境，支持SQL查询和表操作。
     * 抽象处理逻辑：定义抽象方法 handle，供子类实现具体的数据处理逻辑。
     * 读取Kafka数据：通过 readOdsDb 方法，从Kafka中读取数据，创建Flink SQL表。
     * 读取HBase数据：通过 readBaseDic 方法，从HBase中读取数据，创建Flink SQL表。

 * 使用场景：该框架适用于需要从Kafka和HBase中读取数据并进行SQL处理的Flink应用程序。
 * 开发者可以通过继承 BaseSQLApp 类并实现 handle 方法，定义具体的处理逻辑，从而实现复杂的数据处理任务。
 */
public abstract class BaseSQLApp {

    /**
     * 抽象方法，供子类实现具体的处理逻辑
     *
     * @param env  Flink流执行环境
     * @param tEnv Flink表执行环境
     */
    public abstract void handle(StreamExecutionEnvironment env,
                                StreamTableEnvironment tEnv);

    /**
     * 启动Flink SQL应用程序
     *
     * @param port        WebUI绑定的端口
     * @param parallelism 并行度
     * @param ck          检查点存储路径的标识
     */
    public void start(int port, int parallelism, String ck) {
        // 设置操作Hadoop的用户名为Hadoop超级用户atguigu
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // 获取流处理环境，并指定本地测试时启动WebUI所绑定的端口
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度
        env.setParallelism(parallelism);

        // 1. 设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 2. 开启 checkpoint，每5000毫秒触发一次
        env.enableCheckpointing(5000);

        // 3. 设置 checkpoint 模式: 精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 4. checkpoint 存储路径
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall2023/sql/" + ck);

        // 5. checkpoint 并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 6. checkpoint 之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // 7. checkpoint 的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);

        // 8. job 取消的时候, checkpoint 保留策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        // 创建Flink表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 执行具体的处理逻辑
        handle(env, tEnv);
    }

    /**
     * 读取Kafka中的ods_db数据
     *
     * @param tEnv   Flink表执行环境
     * @param groupId 消费组ID
     */
    public void readOdsDb(StreamTableEnvironment tEnv, String groupId){
        tEnv.executeSql("create table topic_db (" +
                "  `database` string, " +
                "  `table` string, " +
                "  `type` string, " +
                "  `data` map<string, string>, " +
                "  `old` map<string, string>, " +
                "  `ts` bigint, " +
                "  `pt` as proctime(), " +
                "  et as to_timestamp_ltz(ts, 0), " +
                "  watermark for et as et - interval '3' second " +
                ")" + SQLUtil.getKafkaDDLSource(groupId, Constant.TOPIC_DB));
    }

    /**
     * 读取HBase中的base_dic数据
     *
     * @param tEnv Flink表执行环境
     */
    public void readBaseDic(StreamTableEnvironment tEnv){
        tEnv.executeSql(
                "create table base_dic (" +
                        " dic_code string," +  // 如果字段是原子类型，则表示这个是 rowKey，字段随意，字段类型随意
                        " info row<dic_name string>, " +  // 字段名和HBase中的列族名保持一致，类型必须是row。嵌套进去的就是列
                        " primary key (dic_code) not enforced " + // 只能用rowKey做主键
                        ") WITH (" +
                        " 'connector' = 'hbase-2.2'," +
                        " 'table-name' = 'gmall:dim_base_dic'," +
                        " 'zookeeper.quorum' = 'hadoop102:2181,hadoop103:2181,hadoop104:2181', " +
                        " 'lookup.cache' = 'PARTIAL', " +
                        " 'lookup.async' = 'true', " +
                        " 'lookup.partial-cache.max-rows' = '20', " +
                        " 'lookup.partial-cache.expire-after-access' = '2 hour' " +
                        ")");
    }
}
