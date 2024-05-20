package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeOrderBean;
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
 * 这段代码通过 Flink 实现了对 Kafka 主题中订单数据的实时处理，
 * 包括数据过滤、字段提取、多表关联和结果写入 Doris 数据库。通过这种方式，
 * 可以高效地统计用户每天的下单数据，为后续的数据分析和处理提供支持。
 */
public class DwsTradeOrderWindow extends BaseApp {
    // 主方法，程序入口
    public static void main(String[] args) {
        new DwsTradeOrderWindow().start(
                10028,  // 应用程序的端口号
                4,      // 并行度
                "dws_trade_order_window",  // 应用程序名称
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL  // Kafka 主题名称
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream
                .map(JSONObject::parseObject)  // 将字符串解析为 JSON 对象
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))  // 允许 5 秒的延迟
                                .withTimestampAssigner((obj, ts) -> obj.getLong("ts") * 1000)  // 提取时间戳，转换为毫秒
                                .withIdleness(Duration.ofSeconds(120L))  // 设置空闲超时时间为 120 秒
                )
                .keyBy(obj -> obj.getString("user_id"))  // 按 user_id 进行分区
                .process(new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {

                    // 定义状态变量，用于存储用户最后一次下单的日期
                    private ValueState<String> lastOrderDateState;

                    @Override
                    public void open(Configuration parameters) {
                        // 初始化状态变量
                        lastOrderDateState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastOrderDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject value,
                                               Context ctx,
                                               Collector<TradeOrderBean> out) throws Exception {
                        long ts = value.getLong("ts") * 1000;

                        String today = DateFormatUtil.tsToDate(ts);
                        String lastOrderDate = lastOrderDateState.value();

                        long orderUu = 0L;
                        long orderNew = 0L;
                        if (!today.equals(lastOrderDate)) {
                            orderUu = 1L;
                            lastOrderDateState.update(today);

                            if (lastOrderDate == null) {
                                orderNew = 1L;
                            }
                        }
                        if (orderUu == 1) {
                            out.collect(new TradeOrderBean("", "", "", orderUu, orderNew, ts));
                        }
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))  // 按照 5 秒的滚动窗口进行聚合
                .reduce(
                        new ReduceFunction<TradeOrderBean>() {
                            @Override
                            public TradeOrderBean reduce(TradeOrderBean value1,
                                                         TradeOrderBean value2) {
                                // 聚合函数，将两个 TradeOrderBean 对象的 orderUniqueUserCount 和 orderNewUserCount 相加
                                value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                                value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                            @Override
                            public void process(Context ctx,
                                                Iterable<TradeOrderBean> elements,
                                                Collector<TradeOrderBean> out) throws Exception {
                                TradeOrderBean bean = elements.iterator().next();
                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));

                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));

                                out.collect(bean);
                            }
                        }
                )
                .map(new DorisMapFunction<>())  // 将数据映射到 Doris 数据库格式
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABASE + ".dws_trade_order_window", "dws_trade_order_window"));  // 将结果写入 Doris 数据库
    }
}
