package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeTrademarkCategoryUserRefundBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.AsyncDimFunction;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DwsTradeTrademarkCategoryUserRefundWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeTrademarkCategoryUserRefundWindow().start(
                10031,  // 应用的端口号
                4,      // 并行度
                "dws_trade_trademark_category_user_refund_window",  // 应用名称
                Constant.TOPIC_DWD_TRADE_ORDER_REFUND  // Kafka 主题
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 将数据流中的 JSON 字符串转换为 TradeTrademarkCategoryUserRefundBean 对象
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanStream = stream
                .map(new MapFunction<String, TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean map(String value) {
                        JSONObject obj = JSON.parseObject(value);
                        return TradeTrademarkCategoryUserRefundBean.builder()
                                .orderIdSet(new HashSet<>(Collections.singleton(obj.getString("order_id"))))
                                .skuId(obj.getString("sku_id"))
                                .userId(obj.getString("user_id"))
                                .ts(obj.getLong("ts") * 1000)
                                .build();
                    }
                });

        // 异步补充 SKU 维度信息
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reducedStream = AsyncDataStream
                .unorderedWait(
                        beanStream,
                        new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                            @Override
                            public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                                return bean.getSkuId();
                            }

                            @Override
                            public String getTableName() {
                                return "dim_sku_info";  // 维度表名称
                            }

                            @Override
                            public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                                bean.setTrademarkId(dim.getString("tm_id"));  // 设置商标 ID
                                bean.setCategory3Id(dim.getString("category3_id"));  // 设置三级分类 ID
                            }
                        },
                        120,  // 超时时间
                        TimeUnit.SECONDS
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())  // 设置时间戳分配器
                                .withIdleness(Duration.ofSeconds(120L))  // 设置空闲超时时间
                )
                // 按用户 ID、三级分类 ID 和商标 ID 分组
                .keyBy(bean -> bean.getUserId() + "_" + bean.getCategory3Id() + "_" + bean.getTrademarkId())
                // 开窗，窗口大小为 5 秒
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                // 聚合操作，将相同 key 的订单集合合并
                .reduce(
                        new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                            @Override
                            public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                                return value1;
                            }
                        },
                        // 处理窗口中的数据，设置开始时间、结束时间、当前日期和退款数量
                        new ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context ctx, Iterable<TradeTrademarkCategoryUserRefundBean> elements, Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {
                                TradeTrademarkCategoryUserRefundBean bean = elements.iterator().next();
                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));  // 设置窗口开始时间
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));  // 设置窗口结束时间
                                bean.setCurDate(DateFormatUtil.tsToDate(ctx.window().getStart()));  // 设置当前日期
                                bean.setRefundCount((long) bean.getOrderIdSet().size());  // 设置退款数量
                                out.collect(bean);  // 输出结果
                            }
                        }
                );

        // 异步补充商标维度信息
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tmStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        return bean.getTrademarkId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";  // 维度表名称
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                        bean.setTrademarkName(dim.getString("tm_name"));  // 设置商标名称
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        // 异步补充三级分类维度信息
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c3Stream = AsyncDataStream.unorderedWait(
                tmStream,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        return bean.getCategory3Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category3";  // 维度表名称
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                        bean.setCategory3Name(dim.getString("name"));  // 设置三级分类名称
                        bean.setCategory2Id(dim.getString("category2_id"));  // 设置二级分类 ID
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        // 异步补充二级分类维度信息
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c2Stream = AsyncDataStream.unorderedWait(
                c3Stream,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        return bean.getCategory2Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category2";  // 维度表名称
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                        bean.setCategory2Name(dim.getString("name"));  // 设置二级分类名称
                        bean.setCategory1Id(dim.getString("category1_id"));  // 设置一级分类 ID
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        // 异步补充一级分类维度信息
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> resultStream = AsyncDataStream.unorderedWait(
                c2Stream,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        return bean.getCategory1Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category1";  // 维度表名称
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                        bean.setCategory1Name(dim.getString("name"));  // 设置一级分类名称
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        // 将结果映射为 Doris 数据格式并输出到 Doris
        resultStream
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABASE + ".dws_trade_trademark_category_user_refund_window", "dws_trade_trademark_category_user_refund_window"));
    }
}
