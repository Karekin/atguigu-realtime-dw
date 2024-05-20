package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeProvinceOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.AsyncDimFunction;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * 这段代码通过 Flink 实现了对 Kafka 主题中订单数据的实时处理，
 * 包括数据过滤、去重、字段提取、多表关联和结果写入 Doris 数据库。通过这种方式，
 * 可以高效地统计每个省份的订单数据，为后续的数据分析和处理提供支持。
 */
public class DwsTradeProvinceOrderWindow extends BaseApp {
    // 主方法，程序入口
    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().start(
                10020,  // 应用程序的端口号
                4,      // 并行度
                "dws_trade_province_order_window",  // 应用程序名称
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL  // Kafka 主题名称
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<TradeProvinceOrderBean> reducedStream = stream
                .map(new MapFunction<String, TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean map(String value) throws Exception {
                        JSONObject obj = JSON.parseObject(value);

                        // 将订单 ID 放入集合中，用于后续的去重
                        HashSet<String> set = new HashSet<>();
                        set.add(obj.getString("order_id"));

                        // 创建并返回 TradeProvinceOrderBean 对象
                        return TradeProvinceOrderBean.builder()
                                .orderDetailId(obj.getString("id"))
                                .orderAmount(obj.getBigDecimal("split_total_amount"))
                                .provinceId(obj.getString("province_id"))
                                .ts(obj.getLong("ts") * 1000)
                                .orderIdSet(set)
                                .build();
                    }

                })
                .keyBy(TradeProvinceOrderBean::getOrderDetailId)  // 按照详情 id 进行分区
                .process(new KeyedProcessFunction<String, TradeProvinceOrderBean, TradeProvinceOrderBean>() {

                    // 定义状态变量，用于存储是否是第一次处理该订单详情
                    private ValueState<Boolean> isFirstState;

                    @Override
                    public void open(Configuration parameters) {
                        // 初始化状态变量
                        isFirstState = getRuntimeContext().getState(new ValueStateDescriptor<>("isFirst", Boolean.class));
                    }

                    @Override
                    public void processElement(TradeProvinceOrderBean value,
                                               Context ctx,
                                               Collector<TradeProvinceOrderBean> out) throws Exception {
                        // 仅处理第一次出现的订单详情
                        if (isFirstState.value() == null) {
                            isFirstState.update(true);
                            out.collect(value);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeProvinceOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))  // 允许 5 秒的延迟
                                .withTimestampAssigner((bean, ts) -> bean.getTs())  // 提取时间戳
                                .withIdleness(Duration.ofSeconds(120L))  // 设置空闲超时时间为 120 秒
                )
                .keyBy(TradeProvinceOrderBean::getProvinceId)  // 按照省份 ID 进行分区
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))  // 按照 5 秒的滚动窗口进行聚合
                .reduce(
                        new ReduceFunction<TradeProvinceOrderBean>() {
                            @Override
                            public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1,
                                                                 TradeProvinceOrderBean value2) {
                                // 聚合函数，将两个 TradeProvinceOrderBean 对象的订单金额和订单 ID 集合合并
                                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String provinceId,
                                                Context ctx,
                                                Iterable<TradeProvinceOrderBean> elements,
                                                Collector<TradeProvinceOrderBean> out) {
                                // 处理窗口结果，设置窗口的开始时间、结束时间和当前日期
                                TradeProvinceOrderBean bean = elements.iterator().next();

                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));

                                bean.setOrderCount((long) bean.getOrderIdSet().size());
                                out.collect(bean);
                            }
                        }
                );

        // 异步方式补充维度信息
        AsyncDataStream
                .unorderedWait(
                        reducedStream,
                        new AsyncDimFunction<TradeProvinceOrderBean>() {
                            @Override
                            public String getRowKey(TradeProvinceOrderBean bean) {
                                return bean.getProvinceId();
                            }

                            @Override
                            public String getTableName() {
                                return "dim_base_province";
                            }

                            @Override
                            public void addDims(TradeProvinceOrderBean bean, JSONObject dim) {
                                bean.setProvinceName(dim.getString("name"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                )
                .map(new DorisMapFunction<>())  // 将数据映射到 Doris 数据库格式
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABASE + ".dws_trade_province_order_window", "dws_trade_province_order_window"));  // 将结果写入 Doris 数据库
    }
}
