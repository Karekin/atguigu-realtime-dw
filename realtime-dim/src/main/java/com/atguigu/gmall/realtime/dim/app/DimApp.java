package com.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import com.atguigu.gmall.realtime.dim.function.HBaseSinkFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.*;

/**
 * 这段代码实现了一个复杂的 Flink 应用程序，用于处理维度数据，并通过多种方式进行数据清洗、表管理和数据写入。
     * 数据清洗：过滤和清洗原始数据，只保留符合条件的数据。
     * 配置表读取：通过 Flink CDC 读取 MySQL 中的配置表数据。
     * HBase 表管理：根据配置表数据在 HBase 中创建或删除表。
     * 数据处理和写入：连接主数据流和广播流，处理数据并将结果写入 HBase。
 */
@Slf4j
public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(10001,
                4,
                "dim_app",
                Constant.TOPIC_DB
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        // 1. 对消费的数据, 做数据清洗
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
        // 2. 通过 flink cdc 读取配置表的数据
        SingleOutputStreamOperator<TableProcessDim> configStream = readTableProcess(env);
        // 3. 根据配置表的数据, 在 HBase 中建表
        configStream = createHBaseTable(configStream);
        // 4. 主流 connect 配置流，并对关联后的流做处理
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDataToTpStream = connect(etlStream, configStream);
        // 5. 删除不需要的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> resultStream = deleteNotNeedColumns(dimDataToTpStream);
        // 6. 写出到 HBase 目标表
        writeToHBase(resultStream);
    }

    private void writeToHBase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> resultStream) {
        /*
        1. 没有专门的 HBase 连接器
        2. SQL 有专门的 HBase 连接器, 由于一次只能写到一个表中, 所以也不能把流转成表再写
        3. 自定义 sink
        */
        resultStream.addSink(new HBaseSinkFunction());
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> deleteNotNeedColumns(
            SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDataToTpStream) {
        return dimDataToTpStream
                .map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
                    @Override
                    public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> dataWithConfig) {
                        JSONObject data = dataWithConfig.f0;
                        List<String> columns = new ArrayList<>(Arrays.asList(dataWithConfig.f1.getSinkColumns().split(",")));
                        columns.add("op_type");

                        data.keySet().removeIf(key -> !columns.contains(key));
                        return dataWithConfig;
                    }
                });
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) {
                        try {
                            JSONObject jsonObj = JSON.parseObject(value);
                            String db = jsonObj.getString("database");
                            String type = jsonObj.getString("type");
                            String data = jsonObj.getString("data");

                            return "gmall".equals(db)
                                    && ("insert".equals(type)
                                    || "update".equals(type)
                                    || "delete".equals(type)
                                    || "bootstrap-insert".equals(type))
                                    && data != null
                                    && data.length() > 2;

                        } catch (Exception e) {
                            log.warn("不是正确的 json 格式的数据: " + value);
                            return false;
                        }

                    }
                })
                .map(JSON::parseObject);
    }

    private SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        // 创建 Properties 对象，用于设置 MySQL 连接的属性
        Properties props = new Properties();
        props.setProperty("useSSL", "false"); // 禁用 SSL 连接
        props.setProperty("allowPublicKeyRetrieval", "true"); // 允许公钥检索

        // 创建 MySqlSource 对象，用于从 MySQL 中读取数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST) // 设置 MySQL 主机地址
                .port(Constant.MYSQL_PORT) // 设置 MySQL 端口号
                .databaseList("gmall2023_config") // 设置要读取的数据库
                .tableList("gmall2023_config.table_process_dim") // 设置要读取的表
                .username(Constant.MYSQL_USER_NAME) // 设置 MySQL 用户名
                .password(Constant.MYSQL_PASSWORD) // 设置 MySQL 密码
                .jdbcProperties(props) // 设置 MySQL 连接属性
                .deserializer(new JsonDebeziumDeserializationSchema()) // 设置反序列化器，将 SourceRecord 转换为 JSON 字符串
                .startupOptions(StartupOptions.initial()) // 设置启动选项，初次启动时读取所有数据
                .build();

        // 使用创建的 MySqlSource 构建数据流
        return env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "cdc-source") // 从 MySqlSource 构建数据流
                .setParallelism(1) // 设置并行度为 1
                .map(new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String value) throws Exception {
                        JSONObject obj = JSON.parseObject(value); // 将 JSON 字符串解析为 JSONObject 对象
                        String op = obj.getString("op"); // 获取操作类型
                        TableProcessDim tableProcessDim;
                        if ("d".equals(op)) { // 如果操作类型是删除
                            tableProcessDim = obj.getObject("before", TableProcessDim.class); // 获取删除前的数据
                        } else { // 其他操作类型（插入或更新）
                            tableProcessDim = obj.getObject("after", TableProcessDim.class); // 获取操作后的数据
                        }
                        tableProcessDim.setOp(op); // 设置操作类型

                        return tableProcessDim; // 返回 TableProcessDim 对象
                    }
                })
                .setParallelism(1); // 设置并行度为 1
    }


    private SingleOutputStreamOperator<TableProcessDim> createHBaseTable(
            SingleOutputStreamOperator<TableProcessDim> tpStream) {
        return tpStream.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {

                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 获取 HBase 的连接
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        // 关闭连接
                        HBaseUtil.closeHBaseConn(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
                        String op = tableProcessDim.getOp();
                        if ("d".equals(op)) {
                            dropTable(tableProcessDim);
                        } else if ("r".equals(op) || "c".equals(op)) {
                            createTable(tableProcessDim);
                        } else {
                            dropTable(tableProcessDim);
                            createTable(tableProcessDim);
                        }
                        return tableProcessDim;
                    }

                    private void createTable(TableProcessDim tableProcessDim) throws IOException {
                        HBaseUtil.createHBaseTable(hbaseConn,
                                Constant.HBASE_NAMESPACE,
                                tableProcessDim.getSinkTable(),
                                tableProcessDim.getSinkFamily());
                    }

                    private void dropTable(TableProcessDim tableProcessDim) throws IOException {
                        HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable());
                    }
                })
                .setParallelism(1);
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(
            SingleOutputStreamOperator<JSONObject> dataStream,
            SingleOutputStreamOperator<TableProcessDim> configStream) {

        // 将配置流转换为广播流
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<>("table_process_dim", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastStream = configStream.broadcast(mapStateDescriptor);

        // 数据流连接广播流
        return dataStream
                .connect(broadcastStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {

                    private HashMap<String, TableProcessDim> map;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化 map 并从 MySQL 中查询 table_process 表所有数据
                        map = new HashMap<>();
                        java.sql.Connection mysqlConn = JdbcUtil.getMysqlConnection();
                        List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(mysqlConn,
                                "select * from gmall2023_config.table_process_dim",
                                TableProcessDim.class,
                                true
                        );

                        for (TableProcessDim tableProcessDim : tableProcessDimList) {
                            String key = tableProcessDim.getSourceTable();
                            map.put(key, tableProcessDim);
                        }
                        JdbcUtil.closeConnection(mysqlConn);
                    }

                    // 处理广播流中的数据: 将配置信息存入广播状态中
                    @Override
                    public void processBroadcastElement(TableProcessDim tableProcessDim,
                                                        Context context,
                                                        Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                        BroadcastState<String, TableProcessDim> state = context.getBroadcastState(mapStateDescriptor);
                        String key = tableProcessDim.getSourceTable();

                        if ("d".equals(tableProcessDim.getOp())) {
                            state.remove(key);
                            map.remove(key);
                        } else {
                            state.put(key, tableProcessDim);
                        }
                    }

                    // 处理数据流中的数据: 从广播状态中读取配置信息
                    @Override
                    public void processElement(JSONObject jsonObj,
                                               ReadOnlyContext context,
                                               Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                        ReadOnlyBroadcastState<String, TableProcessDim> state = context.getBroadcastState(mapStateDescriptor);
                        String key = jsonObj.getString("table");
                        TableProcessDim tableProcessDim = state.get(key);

                        if (tableProcessDim == null) {
                            tableProcessDim = map.get(key);
                            if (tableProcessDim != null) {
                                log.info("在 map 中查找到 " + key);
                            }
                        } else {
                            log.info("在 状态 中查找到 " + key);
                        }
                        if (tableProcessDim != null) {
                            JSONObject data = jsonObj.getJSONObject("data");
                            data.put("op_type", jsonObj.getString("type"));
                            out.collect(Tuple2.of(data, tableProcessDim));
                        }
                    }
                });
    }
}
