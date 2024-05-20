package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradePaymentBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 这段代码通过 Flink 实现了对 Kafka 主题中支付成功数据的实时处理，
 * 包括数据过滤、字段提取、多表关联和结果写入 Doris 数据库。通过这种方式，
 * 可以高效地统计每天的支付成功用户数和新支付用户数，为后续的数据分析和处理提供支持。
 */
public class DwsTradePaymentSucWindow extends BaseApp {
    // 主方法，程序入口
    public static void main(String[] args) {
        new DwsTradePaymentSucWindow().start(
                10027,  // 应用程序的端口号
                4,      // 并行度
                "dws_trade_payment_suc_window",  // 应用程序名称
                Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS  // Kafka 主题名称
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream
                .map(JSON::parseObject)  // 将字符串解析为 JSON 对象
                .keyBy(obj -> obj.getString("user_id"))  // 按 user_id 进行分区
                .process(new KeyedProcessFunction<String, JSONObject, TradePaymentBean>() {

                    // 定义状态变量，用于存储用户最后一次支付成功的日期
                    private ValueState<String> lastPayDateState;

                    @Override
                    public void open(Configuration parameters) {
                        // 初始化状态变量
                        lastPayDateState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastPayDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject obj,
                                               Context ctx,
                                               Collector<TradePaymentBean> out) throws Exception {
                        // 获取状态中存储的最后支付日期
                        String lastPayDate = lastPayDateState.value();
                        long ts = obj.getLong("ts") * 1000;
                        String today = DateFormatUtil.tsToDate(ts);

                        long payUuCount = 0L;
                        long payNewCount = 0L;
                        if (!today.equals(lastPayDate)) {  // 今天第一次支付成功
                            lastPayDateState.update(today);
                            payUuCount = 1L;

                            if (lastPayDate == null) {
                                // 表示这个用户曾经没有支付过, 是一个新用户支付
                                payNewCount = 1L;
                            }
                        }

                        if (payUuCount == 1) {
                            out.collect(new TradePaymentBean("", "", "", payUuCount, payNewCount, ts));
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradePaymentBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))  // 允许 5 秒的乱序
                                .withTimestampAssigner((bean, ts) -> bean.getTs())  // 提取时间戳
                                .withIdleness(Duration.ofSeconds(120L))  // 设置空闲超时时间为 120 秒
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))  // 按照 5 秒的滚动窗口进行聚合
                .reduce(
                        new ReduceFunction<TradePaymentBean>() {
                            @Override
                            public TradePaymentBean reduce(TradePaymentBean value1,
                                                           TradePaymentBean value2) {
                                // 聚合函数，将两个 TradePaymentBean 对象的支付成功新用户数和支付成功独立用户数相加
                                value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                                value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>() {
                            @Override
                            public void process(Context ctx,
                                                Iterable<TradePaymentBean> elements,
                                                Collector<TradePaymentBean> out) throws Exception {
                                // 处理窗口结果，设置窗口的开始时间、结束时间和当前日期
                                TradePaymentBean bean = elements.iterator().next();
                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));

                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));

                                out.collect(bean);
                            }
                        }
                )
                .map(new DorisMapFunction<>())  // 将数据映射到 Doris 数据库格式
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABASE + ".dws_trade_payment_suc_window", "dws_trade_payment_suc_window"));  // 将结果写入 Doris 数据库
    }
}
