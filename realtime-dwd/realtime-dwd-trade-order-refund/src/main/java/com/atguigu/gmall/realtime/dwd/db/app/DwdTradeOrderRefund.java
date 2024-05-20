package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 这段代码通过 Flink 实现了对 Kafka 主题中订单退款数据的实时处理，
 * 包括数据过滤、字段提取、多表关联和结果写入 Kafka。通过这种方式，
 * 可以高效地处理和管理订单退款数据，为后续的数据分析和处理提供支持。
 */
public class DwdTradeOrderRefund extends BaseSQLApp {

    // 主方法，程序入口
    public static void main(String[] args) {
        new DwdTradeOrderRefund().start(
                10017,  // 应用程序的端口号
                4,      // 并行度
                Constant.TOPIC_DWD_TRADE_ORDER_REFUND  // Kafka 主题名称
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 设置空闲状态保留时间为 5 秒
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // 1. 从 Kafka 主题中读取数据，建立动态表
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_ORDER_REFUND);

        // 2. 读取字典表数据
        readBaseDic(tEnv);

        // 3. 过滤订单退款信息数据
        Table orderRefundInfo = tEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['user_id'] user_id," +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['refund_type'] refund_type," +
                        "data['refund_num'] refund_num," +
                        "data['refund_amount'] refund_amount," +
                        "data['refund_reason_type'] refund_reason_type," +
                        "data['refund_reason_txt'] refund_reason_txt," +
                        "data['create_time'] create_time," +
                        "pt," +
                        "ts " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_refund_info' " +
                        "and `type`='insert' ");
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // 4. 过滤订单表中的退单数据
        Table orderInfo = tEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['province_id'] province_id," +
                        "`old` " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_info' " +
                        "and `type`='update'" +
                        "and `old`['order_status'] is not null " +
                        "and `data`['order_status']='1005' ");  // 状态为已退款
        tEnv.createTemporaryView("order_info", orderInfo);

        // 5. 关联退款信息表和订单表，并与字典表进行关联
        Table result = tEnv.sqlQuery(
                "select " +
                        "ri.id," +
                        "ri.user_id," +
                        "ri.order_id," +
                        "ri.sku_id," +
                        "oi.province_id," +
                        "date_format(ri.create_time,'yyyy-MM-dd') date_id," +
                        "ri.create_time," +
                        "ri.refund_type," +
                        "dic1.info.dic_name as refund_type_name," +
                        "ri.refund_reason_type," +
                        "dic2.info.dic_name as refund_reason_type_name," +
                        "ri.refund_reason_txt," +
                        "ri.refund_num," +
                        "ri.refund_amount," +
                        "ri.ts " +
                        "from order_refund_info ri " +
                        "join order_info oi " +
                        "on ri.order_id=oi.id " +
                        "join base_dic for system_time as of ri.pt as dic1 " +
                        "on ri.refund_type=dic1.dic_code " +
                        "join base_dic for system_time as of ri.pt as dic2 " +
                        "on ri.refund_reason_type=dic2.dic_code ");

        // 6. 将结果数据写出到 Kafka
        tEnv.executeSql(
                "create table dwd_trade_order_refund(" +
                        "id string," +
                        "user_id string," +
                        "order_id string," +
                        "sku_id string," +
                        "province_id string," +
                        "date_id string," +
                        "create_time string," +
                        "refund_type_code string," +
                        "refund_type_name string," +
                        "refund_reason_type_code string," +
                        "refund_reason_type_name string," +
                        "refund_reason_txt string," +
                        "refund_num string," +
                        "refund_amount string," +
                        "ts bigint " +
                        ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_ORDER_REFUND));

        // 执行插入操作，将结果数据写入 Kafka 主题
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
    }
}
