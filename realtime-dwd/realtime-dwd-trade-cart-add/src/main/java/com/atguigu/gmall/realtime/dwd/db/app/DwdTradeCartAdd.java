package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 这段代码通过 Flink 实现了对 Kafka 主题中购物车加购数据的实时处理，
 * 包括数据过滤、字段提取和结果写入 Kafka。通过这种方式，
 * 可以高效地处理和管理用户购物车中的加购操作，为后续的数据分析和处理提供支持。
 */
public class DwdTradeCartAdd extends BaseSQLApp {

    // 主方法，程序入口
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(
                10013,  // 应用程序的端口号
                4,      // 并行度
                Constant.TOPIC_DWD_TRADE_CART_ADD  // Kafka 主题名称
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 从 Kafka 主题中读取数据，建立动态表
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_CART_ADD);

        // 2. 过滤出购物车加购数据，并进行必要的字段提取和计算
        Table cartAdd = tEnv.sqlQuery(
                "select " +
                        " `data`['id'] id," +
                        " `data`['user_id'] user_id," +
                        " `data`['sku_id'] sku_id," +
                        // 如果是插入操作，直接取 `data` 中的 `sku_num`，否则计算新旧 `sku_num` 之差
                        " if(`type`='insert'," +
                        "   cast(`data`['sku_num'] as int), " +
                        "   cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int)" +
                        ") sku_num ," +
                        " ts " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='cart_info' " +
                        "and (" +
                        " `type`='insert' " +
                        "  or(" +
                        "     `type`='update' " +
                        "      and `old`['sku_num'] is not null " +
                        "      and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int) " +
                        "   )" +
                        ")");

        // 3. 创建 Kafka Sink 表，用于将处理后的数据写入 Kafka
        tEnv.executeSql("create table dwd_trade_cart_add(" +
                "   id string, " +
                "   user_id string," +
                "   sku_id string," +
                "   sku_num int, " +
                "   ts  bigint " +
                ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_CART_ADD));

        // 将过滤和计算后的加购数据写入到 Kafka Sink 表
        cartAdd.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}
