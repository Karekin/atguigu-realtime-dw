package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 这段代码通过 Flink 实现了对 Kafka 主题中订单支付成功数据的实时处理，
 * 包括数据过滤、字段提取、多表关联和结果写入 Kafka。通过这种方式，
 * 可以高效地处理和管理订单支付成功数据，为后续的数据分析和处理提供支持。
 */
public class DwdTradeOrderPaySucDetail extends BaseSQLApp {

    // 主方法，程序入口
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(
                10016,  // 应用程序的端口号
                4,      // 并行度
                Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS  // Kafka 主题名称
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 读取下单事务事实表
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
                        "ts bigint," +
                        "et as to_timestamp_ltz(ts, 0), " +  // 事件时间
                        "watermark for et as et - interval '3' second " +  // 水印
                        ")" + SQLUtil.getKafkaDDLSource(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

        // 2. 读取 Kafka 数据
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);

        // 3. 读取字典表
        readBaseDic(tEnv);

        // 4. 从 Kafka 主题中过滤支付信息
        Table paymentInfo = tEnv.sqlQuery("select " +
                "data['user_id'] user_id," +
                "data['order_id'] order_id," +
                "data['payment_type'] payment_type," +
                "data['callback_time'] callback_time," +
                "`pt`," +
                "ts, " +
                "et " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='payment_info' " +
                "and `type`='update' " +
                "and `old`['payment_status'] is not null " +
                "and `data`['payment_status']='1602' ");  // 支付成功的订单
        tEnv.createTemporaryView("payment_info", paymentInfo);

        // 5. 进行多表关联 (Interval Join)
        Table result = tEnv.sqlQuery(
                "select " +
                        "od.id order_detail_id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "pi.payment_type payment_type_code ," +
                        "dic.dic_name payment_type_name," +
                        "pi.callback_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount split_payment_amount," +
                        "pi.ts " +
                        "from payment_info pi " +
                        "join dwd_trade_order_detail od " +
                        "on pi.order_id=od.order_id " +
                        "and od.et >= pi.et - interval '30' minute " +  // 关联条件，支付成功时间与订单创建时间在30分钟内
                        "and od.et <= pi.et + interval '5' second " +  // 关联条件，支付成功时间与订单创建时间在5秒内
                        "join base_dic for system_time as of pi.pt as dic " +  // 系统时间关联字典表
                        "on pi.payment_type=dic.dic_code ");

        // 6. 将结果数据写出到 Kafka
        tEnv.executeSql("create table dwd_trade_order_pay_suc_detail(" +
                "order_detail_id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "callback_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_payment_amount string," +
                "ts bigint " +
                ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }
}
