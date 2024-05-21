package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.api.common.state.StateTtlConfig.StateVisibility.NeverReturnExpired;
import static org.apache.flink.api.common.state.StateTtlConfig.UpdateType.OnCreateAndWrite;

/**
 * 作用：实时统计页面浏览数据，包括页面浏览次数（PV）、独立访客数（UV）、会话数（SV）和访问时长（DurSum），并将统计结果写入Doris数据库。
 * 目标：通过实时流处理技术，实现对用户行为的实时监控和分析，获取详细的页面浏览数据，提高数据处理的实时性和准确性。
 */
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    // main 方法是程序的入口，设置了应用的参数并启动应用
    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(
                10022,  // 应用ID
                4,      // 并行度
                "dws_traffic_vc_ch_ar_is_new_page_view_window", // 应用名称
                Constant.TOPIC_DWD_TRAFFIC_PAGE // Kafka主题
        );
    }

    // 重写 handle 方法，用于处理流数据
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 解析数据, 封装成 POJO 对象
        SingleOutputStreamOperator<TrafficPageViewBean> beanStream = parseToPojo(stream);
        // 2. 开窗聚合
        SingleOutputStreamOperator<TrafficPageViewBean> result = windowAndAgg(beanStream);
        // 3. 写出到 Doris 中
        writeToDoris(result);
    }

    // 定义 writeToDoris 方法，将结果写入 Doris 数据库
    private void writeToDoris(SingleOutputStreamOperator<TrafficPageViewBean> result) {
        result
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABASE + ".dws_traffic_vc_ch_ar_is_new_page_view_window", "dws_traffic_vc_ch_ar_is_new_page_view_window"));
    }

    // 定义 windowAndAgg 方法，对数据进行开窗和聚合操作
    private SingleOutputStreamOperator<TrafficPageViewBean> windowAndAgg(SingleOutputStreamOperator<TrafficPageViewBean> beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(120L))
                )
                .keyBy(bean -> bean.getVc() + "_" + bean.getCh() + "_" + bean.getAr() + "_" + bean.getIsNew())
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<TrafficPageViewBean>() {
                            @Override
                            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                                value1.setPvCt(value1.getPvCt() + value2.getPvCt()); // 聚合页面浏览次数
                                value1.setUvCt(value1.getUvCt() + value2.getUvCt()); // 聚合独立访客数
                                value1.setSvCt(value1.getSvCt() + value2.getSvCt()); // 聚合会话数
                                value1.setDurSum(value1.getDurSum() + value2.getDurSum()); // 聚合访问时长
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context ctx, Iterable<TrafficPageViewBean> elements, Collector<TrafficPageViewBean> out) throws Exception {
                                TrafficPageViewBean bean = elements.iterator().next();
                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart())); // 设置窗口开始时间
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd())); // 设置窗口结束时间
                                bean.setCur_date(DateFormatUtil.tsToDateForPartition(ctx.window().getStart())); // 设置当前日期分区
                                out.collect(bean); // 输出聚合结果
                            }
                        }
                );
    }

    // 定义 parseToPojo 方法，将 JSON 字符串解析为 POJO 对象
    private SingleOutputStreamOperator<TrafficPageViewBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"))  // 按照 mid 分组，计算独立访客数
                .process(new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {
                    private ValueState<String> lastVisitDateState; // 定义状态，用于记录上次访问日期

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastVisitDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject obj, Context ctx, Collector<TrafficPageViewBean> out) throws Exception {
                        JSONObject page = obj.getJSONObject("page");
                        JSONObject common = obj.getJSONObject("common");
                        Long ts = obj.getLong("ts");
                        String today = DateFormatUtil.tsToDate(ts);

                        Long pv = 1L; // 页面浏览次数
                        Long durSum = page.getLong("during_time"); // 访问时长
                        // 计算独立访客数
                        String lastVisitDate = lastVisitDateState.value();
                        Long uv = 0L;
                        if (!today.equals(lastVisitDate)) {
                            uv = 1L; // 当天第一次访问，独立访客数为1
                            lastVisitDateState.update(today); // 更新状态
                        }

                        TrafficPageViewBean bean = new TrafficPageViewBean();
                        bean.setVc(common.getString("vc")); // 版本号
                        bean.setCh(common.getString("ch")); // 渠道
                        bean.setAr(common.getString("ar")); // 地区
                        bean.setIsNew(common.getString("is_new")); // 是否新用户

                        bean.setPvCt(pv); // 页面浏览次数
                        bean.setUvCt(uv); // 独立访客数
                        bean.setDurSum(durSum); // 访问时长
                        bean.setTs(ts); // 时间戳

                        bean.setSid(common.getString("sid")); // 会话ID

                        out.collect(bean); // 输出结果
                    }
                })
                .keyBy(TrafficPageViewBean::getSid) // 按照会话ID分组，计算会话数
                .process(new KeyedProcessFunction<String, TrafficPageViewBean, TrafficPageViewBean>() {
                    private ValueState<Boolean> isFirstState; // 定义状态，用于记录是否是第一次访问

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("isFirst", Boolean.class);
                        StateTtlConfig conf = new StateTtlConfig.Builder(Time.hours(1))
                                .setStateVisibility(NeverReturnExpired)
                                .setUpdateType(OnCreateAndWrite)
                                .useProcessingTime()
                                .build();
                        desc.enableTimeToLive(conf);
                        isFirstState = getRuntimeContext().getState(desc);
                    }

                    @Override
                    public void processElement(TrafficPageViewBean bean, Context ctx, Collector<TrafficPageViewBean> out) throws Exception {
                        if (isFirstState.value() == null) {
                            bean.setSvCt(1L); // 第一次访问，会话数为1
                            isFirstState.update(true); // 更新状态
                        } else {
                            bean.setSvCt(0L); // 非第一次访问，会话数为0
                        }
                        out.collect(bean); // 输出结果
                    }
                });
    }
}
