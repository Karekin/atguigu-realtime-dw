package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 这段代码通过 Flink 实现了对 Kafka 主题中退款支付成功数据的实时处理，
 * 包括数据过滤、字段提取、多表关联和结果写入 Kafka。通过这种方式，
 * 可以高效地处理和管理退款支付成功数据，为后续的数据分析和处理提供支持。
 */
public class DwdTradeRefundPaySucDetail extends BaseSQLApp {

    // 主方法，程序入口
    public static void main(String[] args) {
        new DwdTradeRefundPaySucDetail().start(
                10018,  // 应用程序的端口号
                4,      // 并行度
                Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS  // Kafka 主题名称
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 设置空闲状态保留时间为 5 秒
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // 1. 从 Kafka 主题中读取数据，建立动态表
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);

        // 2. 读取字典表数据
        readBaseDic(tEnv);

        // 3. 过滤退款支付成功表数据
        Table refundPayment = tEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['payment_type'] payment_type," +
                        "data['callback_time'] callback_time," +
                        "data['total_amount'] total_amount," +
                        "pt, " +
                        "ts " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='refund_payment' " +
                        "and `type`='update' " +
                        "and `old`['refund_status'] is not null " +
                        "and `data`['refund_status']='1602'");
        tEnv.createTemporaryView("refund_payment", refundPayment);

        // 4. 过滤退单表中的退款成功的数据
        Table orderRefundInfo = tEnv.sqlQuery(
                "select " +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['refund_num'] refund_num " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_refund_info' " +
                        "and `type`='update' " +
                        "and `old`['refund_status'] is not null " +
                        "and `data`['refund_status']='0705'");
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // 5. 过滤订单表中的退款成功的数据
        Table orderInfo = tEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['user_id'] user_id," +
                        "data['province_id'] province_id " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_info' " +
                        "and `type`='update' " +
                        "and `old`['order_status'] is not null " +
                        "and `data`['order_status']='1006'");
        tEnv.createTemporaryView("order_info", orderInfo);

        // 6. 将四张表进行 join 操作
        Table result = tEnv.sqlQuery(
                "select " +
                        "rp.id," +
                        "oi.user_id," +
                        "rp.order_id," +
                        "rp.sku_id," +
                        "oi.province_id," +
                        "rp.payment_type," +
                        "dic.info.dic_name payment_type_name," +
                        "date_format(rp.callback_time,'yyyy-MM-dd') date_id," +
                        "rp.callback_time," +
                        "ori.refund_num," +
                        "rp.total_amount as refund_amount," +
                        "rp.ts " +
                        "from refund_payment rp " +
                        "join order_refund_info ori " +
                        "on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " +
                        "join order_info oi " +
                        "on rp.order_id=oi.id " +
                        "join base_dic for system_time as of rp.pt as dic " +
                        "on rp.payment_type=dic.dic_code ");

        // 7. 将结果数据写出到 Kafka
        tEnv.executeSql("create table dwd_trade_refund_pay_suc_detail(" +
                "id string," +
                "user_id string," +
                "order_id string," +
                "sku_id string," +
                "province_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "date_id string," +
                "callback_time string," +
                "refund_num string," +
                "refund_amount string," +
                "ts bigint " +
                ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS));

        // 执行插入操作，将结果数据写入 Kafka 主题
        result.executeInsert(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
    }
}
