package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.CartAddUuBean;
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
 * 这段代码通过 Flink 实现了对 Kafka 主题中购物车添加数据的实时处理，
 * 包括数据过滤、字段提取、多表关联和结果写入 Doris 数据库。通过这种方式，
 * 可以高效地统计用户每天添加购物车的独立用户数，为后续的数据分析和处理提供支持。
 */
public class DwsTradeCartAddUuWindow extends BaseApp {
    // 主方法，程序入口
    public static void main(String[] args) {
        new DwsTradeCartAddUuWindow().start(
                10026,  // 应用程序的端口号
                4,      // 并行度
                "dws_trade_cart_add_uu_window",  // 应用程序名称
                Constant.TOPIC_DWD_TRADE_CART_ADD  // Kafka 主题名称
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream
                .map(JSON::parseObject)  // 将字符串解析为 JSON 对象
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))  // 允许 5 秒的延迟
                                .withTimestampAssigner((obj, ts) -> obj.getLong("ts") * 1000)  // 提取时间戳，转换为毫秒
                                .withIdleness(Duration.ofSeconds(120L))  // 设置空闲超时时间为 120 秒
                )
                .keyBy(obj -> obj.getString("user_id"))  // 按 user_id 进行分区
                .process(new KeyedProcessFunction<String, JSONObject, CartAddUuBean>() {

                    // 定义状态变量，用于存储用户最后一次添加购物车的日期
                    private ValueState<String> lastCartAddDateState;

                    @Override
                    public void open(Configuration parameters) {
                        // 初始化状态变量
                        lastCartAddDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastCartAddDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject jsonObj,
                                               Context context,
                                               Collector<CartAddUuBean> out) throws Exception {
                        // 获取状态中存储的最后添加购物车日期
                        String lastCartAddDate = lastCartAddDateState.value();
                        long ts = jsonObj.getLong("ts") * 1000;
                        String today = DateFormatUtil.tsToDate(ts);

                        // 如果今天还没有添加过购物车
                        if (!today.equals(lastCartAddDate)) {
                            // 更新状态
                            lastCartAddDateState.update(today);
                            // 输出结果
                            out.collect(new CartAddUuBean("", "", "", 1L));
                        }
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))  // 按照 5 秒的滚动窗口进行聚合
                .reduce(
                        new ReduceFunction<CartAddUuBean>() {
                            @Override
                            public CartAddUuBean reduce(CartAddUuBean value1,
                                                        CartAddUuBean value2) {
                                // 聚合函数，将两个 CartAddUuBean 对象的 cartAddUuCt 相加
                                value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                            @Override
                            public void process(Context ctx,
                                                Iterable<CartAddUuBean> elements,
                                                Collector<CartAddUuBean> out) throws Exception {
                                // 处理窗口结果，设置窗口的开始时间、结束时间和当前日期
                                CartAddUuBean bean = elements.iterator().next();
                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));

                                // 输出结果
                                out.collect(bean);
                            }
                        }
                )
                .map(new DorisMapFunction<>())  // 将数据映射到 Doris 数据库格式
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABASE + ".dws_trade_cart_add_uu_window", "dws_trade_cart_add_uu_window"));  // 将结果写入 Doris 数据库
    }
}
