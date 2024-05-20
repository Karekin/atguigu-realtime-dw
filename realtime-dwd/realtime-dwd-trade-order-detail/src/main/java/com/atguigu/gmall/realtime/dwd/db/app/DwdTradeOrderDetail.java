package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 这段代码通过 Flink 实现了对 Kafka 主题中订单详情数据的实时处理，
 * 包括数据过滤、字段提取、多表关联和结果写入 Kafka。通过这种方式，
 * 可以高效地处理和管理订单详情数据，为后续的数据分析和处理提供支持。
 */
public class DwdTradeOrderDetail extends BaseSQLApp {

    // 主方法，程序入口
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(
                10014,  // 应用程序的端口号
                4,      // 并行度
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL  // Kafka 主题名称
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 因为有 join 操作，默认所有表的数据都会一致存储到内存中，所以要设置空闲状态保留时间为 5 秒
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // 1. 从 Kafka 主题中读取数据，建立动态表
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);

        // 2. 过滤出 order_detail 数据，操作类型为 insert
        Table orderDetail = tEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['sku_name'] sku_name," +
                        "data['create_time'] create_time," +
                        "data['source_id'] source_id," +
                        "data['source_type'] source_type," +
                        "data['sku_num'] sku_num," +
                        "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                        "   cast(data['order_price'] as decimal(16,2)) as String) split_original_amount," + // 分摊原始总金额
                        "data['split_total_amount'] split_total_amount," +  // 分摊总金额
                        "data['split_activity_amount'] split_activity_amount," + // 分摊活动金额
                        "data['split_coupon_amount'] split_coupon_amount," + // 分摊的优惠券金额
                        "ts " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_detail' " +
                        "and `type`='insert' ");
        tEnv.createTemporaryView("order_detail", orderDetail);

        // 3. 过滤出 order_info 数据，操作类型为 insert
        Table orderInfo = tEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['user_id'] user_id," +
                        "data['province_id'] province_id " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_info' " +
                        "and `type`='insert' ");
        tEnv.createTemporaryView("order_info", orderInfo);

        // 4. 过滤出 order_detail_activity 数据，操作类型为 insert
        Table orderDetailActivity = tEnv.sqlQuery(
                "select " +
                        "data['order_detail_id'] order_detail_id, " +
                        "data['activity_id'] activity_id, " +
                        "data['activity_rule_id'] activity_rule_id " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_detail_activity' " +
                        "and `type`='insert' ");
        tEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        // 5. 过滤出 order_detail_coupon 数据，操作类型为 insert
        Table orderDetailCoupon = tEnv.sqlQuery(
                "select " +
                        "data['order_detail_id'] order_detail_id, " +
                        "data['coupon_id'] coupon_id " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_detail_coupon' " +
                        "and `type`='insert' ");
        tEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        // 6. 将四张表进行 join 操作
        Table result = tEnv.sqlQuery(
                "select " +
                        "od.id," +
                        "od.order_id," +
                        "oi.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "oi.province_id," +
                        "act.activity_id," +
                        "act.activity_rule_id," +
                        "cou.coupon_id," +
                        "date_format(od.create_time, 'yyyy-MM-dd') date_id," +  // 年月日
                        "od.create_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "od.ts " +
                        "from order_detail od " +
                        "join order_info oi on od.order_id=oi.id " +
                        "left join order_detail_activity act " +
                        "on od.id=act.order_detail_id " +
                        "left join order_detail_coupon cou " +
                        "on od.id=cou.order_detail_id ");

        // 7. 将结果数据写出到 Kafka
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
                        "primary key(id) not enforced " +
                        ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

        // 执行插入操作，将结果数据写入 Kafka 主题
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
}
