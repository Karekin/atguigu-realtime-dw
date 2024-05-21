package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TrafficHomeDetailPageViewBean;
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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 作用：实时统计访问首页和商品详情页的用户访问量，并将统计结果存储到Doris数据库中，供后续数据分析使用。
 * 目标：通过实时流处理技术，实现对网站流量的实时监控和分析，及时获取用户行为数据，提升数据处理的实时性和准确性。
 */
public class DwsTrafficHomeDetailPageViewWindow extends BaseApp {
    // main方法是程序的入口，设置了应用的参数并启动应用
    public static void main(String[] args) {
        new DwsTrafficHomeDetailPageViewWindow().start(
                10023,  // 应用ID
                4,      // 并行度
                "dws_traffic_home_detail_page_view_window", // 应用名称
                Constant.TOPIC_DWD_TRAFFIC_PAGE // Kafka主题
        );
    }

    // 重写handle方法，用于处理流数据
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 过滤, 解析成 pojo 类型
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream = parseToPojo(stream);

        // 2. 开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultStream = windowAndAgg(beanStream);

        // 3. 写出到 doris
        writeToDoris(resultStream);
    }

    // 定义writeToDoris方法，将结果流写入Doris数据库
    private void writeToDoris(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultStream) {
        resultStream
                .map(new DorisMapFunction<>()) // 转换为DorisMapFunction类型
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABASE + ".dws_traffic_home_detail_page_view_window", "dws_traffic_home_detail_page_view_window")); // 写入Doris
    }

    // 定义windowAndAgg方法，对数据流进行开窗和聚合操作
    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> windowAndAgg(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)) // 允许最大5秒的乱序
                                .withTimestampAssigner((bean, ts) -> bean.getTs()) // 指定时间戳
                                .withIdleness(Duration.ofSeconds(120L)) // 指定空闲超时时间
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L))) // 设置滚动窗口，窗口大小为5秒
                .reduce(
                        new ReduceFunction<TrafficHomeDetailPageViewBean>() { // 定义reduce函数，进行聚合操作
                            @Override
                            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1,
                                                                        TrafficHomeDetailPageViewBean value2) throws Exception {
                                value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt()); // 聚合首页访问量
                                value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt()); // 聚合商品详情页访问量
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() { // 定义窗口处理函数
                            @Override
                            public void process(Context ctx,
                                                Iterable<TrafficHomeDetailPageViewBean> elements,
                                                Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                                TrafficHomeDetailPageViewBean bean = elements.iterator().next();
                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart())); // 设置窗口开始时间
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd())); // 设置窗口结束时间
                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getStart())); // 设置当前日期分区
                                out.collect(bean); // 输出聚合结果
                            }
                        }
                );
    }

    // 定义parseToPojo方法，将JSON字符串解析为POJO对象
    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject) // 解析JSON字符串
                .keyBy(obj -> obj.getJSONObject("common").getString("mid")) // 按照mid字段进行分组
                .process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() { // 定义处理函数

                    private ValueState<String> homeState; // 定义首页访问状态
                    private ValueState<String> goodDetailState; // 定义商品详情页访问状态

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        homeState = getRuntimeContext().getState(new ValueStateDescriptor<String>("home", String.class)); // 初始化首页访问状态
                        goodDetailState = getRuntimeContext().getState(new ValueStateDescriptor<String>("goodDetail", String.class)); // 初始化商品详情页访问状态
                    }

                    @Override
                    public void processElement(JSONObject obj,
                                               Context ctx,
                                               Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        String pageId = obj.getJSONObject("page").getString("page_id"); // 获取页面ID
                        Long ts = obj.getLong("ts"); // 获取时间戳
                        String today = DateFormatUtil.tsToDate(ts); // 获取当天日期
                        String lastHomeDate = homeState.value(); // 获取上次首页访问日期
                        String lastGoodDetailDate = goodDetailState.value(); // 获取上次商品详情页访问日期
                        Long homeCt = 0L; // 初始化首页访问量
                        Long goodDetailCt = 0L; // 初始化商品详情页访问量

                        // 判断是否为当天首次访问首页
                        if ("home".equals(pageId) && !today.equals(lastHomeDate)) {
                            homeCt = 1L;
                            homeState.update(today); // 更新首页访问状态
                        } else if ("good_detail".equals(pageId) && !today.equals(lastGoodDetailDate)) { // 判断是否为当天首次访问商品详情页
                            goodDetailCt = 1L;
                            goodDetailState.update(today); // 更新商品详情页访问状态
                        }

                        // 如果有任意一个访问量为1，则输出结果
                        if (homeCt + goodDetailCt == 1) {
                            out.collect(new TrafficHomeDetailPageViewBean(
                                    "", "", "", homeCt, goodDetailCt, ts)); // 输出访问量统计结果
                        }
                    }
                });
    }
}
