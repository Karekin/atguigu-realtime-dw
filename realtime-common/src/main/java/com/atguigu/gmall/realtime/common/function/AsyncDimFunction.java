package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 该代码的主要作用是实现异步维度查询功能，通过异步方式从Redis和HBase中获取维度数据，并将其添加到处理的数据中。具体目标包括：
     * 异步连接的初始化和释放：通过 open 和 close 方法，初始化和释放Redis和HBase的异步连接。
     * 异步维度查询：通过 asyncInvoke 方法，实现异步维度查询逻辑，包括从Redis查询、从HBase查询以及将查询结果写回Redis。
     * 数据处理：在异步查询到维度数据后，将其添加到处理的数据中，并通过 resultFuture 输出结果。
 * @param <T>
 */
@Slf4j
public abstract class AsyncDimFunction<T> extends RichAsyncFunction<T, T> implements DimFunction<T> {

    /*
       定义Redis和HBase的异步连接对象，Lettuce 的设计使其在高并发和分布式环境中表现良好，
       是 Java 应用程序中常用的 Redis 客户端库之一。
     */
    private StatefulRedisConnection<String, String> redisAsyncConn;
    private AsyncConnection hBaseAsyncConn;

    /**
     * 打开连接，初始化Redis和HBase的异步连接
     *
     * @param parameters 配置参数
     * @throws Exception 异常
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        redisAsyncConn = RedisUtil.getRedisAsyncConnection();
        hBaseAsyncConn = HBaseUtil.getHBaseAsyncConnection();
    }

    /**
     * 关闭连接，释放Redis和HBase的异步连接
     *
     * @throws Exception 异常
     */
    @Override
    public void close() throws Exception {
        RedisUtil.closeRedisAsyncConnection(redisAsyncConn);
        HBaseUtil.closeAsyncHbaseConnection(hBaseAsyncConn);
    }

    /**
     * 异步处理方法，每个元素到达时执行一次
     *
     * @param bean         需要异步处理的元素
     * @param resultFuture 异步处理结果的容器
     * @throws Exception 异常
     */
    @Override
    public void asyncInvoke(T bean, ResultFuture<T> resultFuture) throws Exception {

        // 使用CompletableFuture进行异步操作
        CompletableFuture
                // 从 Redis 中异步读取维度数据: CompletableFuture.supplyAsync 方法接受一个 Supplier，并在异步执行该供应器提供的操作。
                .supplyAsync(new Supplier<JSONObject>() {
                    @Override
                    public JSONObject get() {
                        // 从Redis异步读取维度数据
                        return RedisUtil.readDimAsync(redisAsyncConn, getTableName(), getRowKey(bean));
                    }
                })
                // 如果从 Redis 中没有读取到数据，则从 HBase 异步读取数据，并将结果写入 Redis: thenApplyAsync 方法接受一个 Function，用于处理 supplyAsync 返回的结果。
                .thenApplyAsync(new Function<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject apply(JSONObject dimFromRedis) {
                        // 如果Redis中没有读取到数据，则从HBase异步读取维度数据
                        JSONObject dim = dimFromRedis;
                        if (dim == null) {
                            // 从HBase读取维度数据
                            dim = HBaseUtil.readDimAsync(hBaseAsyncConn, "gmall", getTableName(), getRowKey(bean));
                            // 将从HBase读取到的数据写入到Redis中
                            RedisUtil.writeDimAsync(redisAsyncConn, getTableName(), getRowKey(bean), dim);
                            log.info("走的是 hbase " + getTableName() + "  " + getRowKey(bean));
                        } else {
                            log.info("走的是 redis " + getTableName() + "  " + getRowKey(bean));
                        }
                        return dim;
                    }
                })
                // 将维度数据添加到 bean 中，并将结果输出到 resultFuture，以完成异步处理: thenAccept 方法接受一个 Consumer，用于处理前一步操作返回的结果
                .thenAccept(new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject dim) {
                        // 将维度数据添加到bean中
                        addDims(bean, dim);
                        // 将结果输出到resultFuture
                        resultFuture.complete(Collections.singleton(bean));
                    }
                });
    }

}
