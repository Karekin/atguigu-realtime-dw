package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.io.IOException;

/**
 * 实现一个 Flink SinkFunction，用于将数据写入 HBase 数据库，并在 Redis 中进行相应的缓存管理，以支持维度表的增、删、改操作。
 */
@Slf4j
public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {

    // HBase 连接对象
    private Connection conn;

    // Redis 客户端对象
    private Jedis jedis;

    // 在 open 方法中初始化 HBase 和 Redis 的连接
    @Override
    public void open(Configuration parameters) throws Exception {
        conn = HBaseUtil.getHBaseConnection(); // 获取 HBase 连接
        jedis = RedisUtil.getJedis(); // 获取 Redis 客户端
    }

    // 在 close 方法中关闭 HBase 和 Redis 的连接
    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConn(conn); // 关闭 HBase 连接
        RedisUtil.closeJedis(jedis); // 关闭 Redis 客户端
    }

    // 处理每一条输入的数据
    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> dataWithConfig, Context context) throws Exception {
        // 从输入数据中提取 JSON 对象和操作类型
        JSONObject data = dataWithConfig.f0;
        String opType = data.getString("op_type");

        // 根据操作类型执行相应的处理逻辑
        if ("delete".equals(opType)) {
            // 如果是删除操作，调用 delDim 方法删除 HBase 中的数据
            delDim(dataWithConfig);
        } else {
            // 如果是插入、更新或引导插入操作，调用 putDim 方法写入 HBase 中的数据
            putDim(dataWithConfig);
        }

        TableProcessDim tableProcessDim = dataWithConfig.f1;
        // 如果操作类型是更新或删除，则删除 Redis 中的缓存数据
        if ("delete".equals(opType) || "update".equals(opType)) {
            String key = RedisUtil.getKey(
                    tableProcessDim.getSinkTable(),
                    data.getString(tableProcessDim.getSinkRowKey()));
            jedis.del(key);
        }
    }

    // 将数据写入 HBase
    private void putDim(Tuple2<JSONObject, TableProcessDim> dataWithConfig) throws IOException {
        JSONObject data = dataWithConfig.f0;
        TableProcessDim tableProcessDim = dataWithConfig.f1;

        String rowKey = data.getString(tableProcessDim.getSinkRowKey());
        log.info("向 HBase 写入数据 dataWithConfig: " + dataWithConfig);
        // 移除 JSON 对象中的操作类型字段
        data.remove("op_type");
        // 使用 HBase 工具类将数据写入 HBase
        HBaseUtil.putRow(conn,
                Constant.HBASE_NAMESPACE,
                tableProcessDim.getSinkTable(),
                rowKey,
                tableProcessDim.getSinkFamily(),
                data);
    }

    // 从 HBase 中删除数据
    private void delDim(Tuple2<JSONObject, TableProcessDim> dataWithConfig) throws IOException {
        JSONObject data = dataWithConfig.f0;
        TableProcessDim tableProcessDim = dataWithConfig.f1;

        String rowKey = data.getString(tableProcessDim.getSinkRowKey());
        // 使用 HBase 工具类从 HBase 中删除数据
        HBaseUtil.delRow(conn,
                Constant.HBASE_NAMESPACE,
                tableProcessDim.getSinkTable(),
                rowKey);
    }
}
