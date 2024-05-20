package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 这段代码通过 Flink 实现了对 Kafka 主题中订单取消数据的实时处理，
 * 包括数据过滤、字段提取、Join 操作和结果写入 Kafka。通过这种方式，
 * 可以高效地处理和管理订单取消数据，为后续的数据分析和处理提供支持。
 */
public class DwdTradeOrderCancelDetail extends BaseSQLApp {

    // 主方法，程序入口
    public static void main(String[] args) {
        new DwdTradeOrderCancelDetail().start(
                10015,  // 应用程序的端口号
                4,      // 并行度
                Constant.TOPIC_DWD_TRADE_ORDER_CANCEL  // Kafka 主题名称
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 设置空闲状态保留时间为30分钟零5秒
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

        // 1. 从 Kafka 主题中读取数据，建立动态表
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);

        // 2. 读取 DWD 层的下单事务事实表数据
        tEnv.executeSql(
                "create table dwd_trade_order_detail(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "create_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint " +
                        ")" + SQLUtil.getKafkaDDLSource(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

        // 3. 从 topic_db 过滤出订单取消数据
        Table orderCancel = tEnv.sqlQuery("select " +
                " `data`['id'] id, " +
                " `data`['operate_time'] operate_time, " +
                " `ts` " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_info' " +
                "and `type`='update' " +
                "and `old`['order_status']='1001' " +
                "and `data`['order_status']='1003' ");
        // 创建临时视图
        tEnv.createTemporaryView("order_cancel", orderCancel);

        // 4. 订单取消表和下单表进行 join
        Table result = tEnv.sqlQuery(
                "select  " +
                        "od.id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "date_format(oc.operate_time, 'yyyy-MM-dd') order_cancel_date_id," +
                        "oc.operate_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "oc.ts " +
                        "from dwd_trade_order_detail od " +
                        "join order_cancel oc " +
                        "on od.order_id=oc.id ");

        // 5. 写出结果到 Kafka
        tEnv.executeSql(
                "create table dwd_trade_order_cancel_detail(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "cancel_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint " +
                        ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));

        // 执行插入操作，将结果数据写入 Kafka 主题
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }
}
