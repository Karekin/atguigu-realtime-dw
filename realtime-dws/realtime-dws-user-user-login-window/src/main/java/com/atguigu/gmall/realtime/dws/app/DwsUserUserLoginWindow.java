package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.UserLoginBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 这段代码是一个基于 Apache Flink 的实时数据处理应用，主要用于处理用户登录数据，
 * 计算每日活跃用户（DAU）和7日回流用户（Retention Users）。具体流程如下：
     * 从 Kafka 读取登录日志。
     * 过滤出登录相关的记录。
     * 将日志解析为 Java 对象，判断用户是否是首次登录或7日回流用户。
     * 进行窗口聚合计算。
     * 将计算结果写入 Doris 数据库中。
 */
public class DwsUserUserLoginWindow extends BaseApp {
    // main 方法是程序的入口
    public static void main(String[] args) {
        new DwsUserUserLoginWindow().start(
                10024, // Flink 任务的并行度
                4, // 检查点的保存间隔
                "dws_user_user_login_window", // 任务名称
                Constant.TOPIC_DWD_TRAFFIC_PAGE // Kafka 主题
        );
    }

    // handle 方法是处理数据的主流程
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 过滤出所有登录记录
        SingleOutputStreamOperator<JSONObject> loginLogStream = filterLoginLog(stream);

        // 2. 再解析封装到 pojo 中: 如果是当日首次登录, 则置为 1 否则置为 0
        // 回流用户: 判断今天和最后一次登录日期的差值是否大于 7
        SingleOutputStreamOperator<UserLoginBean> beanStream = parseToPojo(loginLogStream);

        // 3. 开窗聚合
        SingleOutputStreamOperator<UserLoginBean> resultStream = windowAndAgg(beanStream);

        // 4. 将结果写入 Doris
        writeToDoris(resultStream);
    }

    // 将结果写入 Doris 数据库的方法
    private void writeToDoris(SingleOutputStreamOperator<UserLoginBean> resultStream) {
        resultStream
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABASE + ".dws_user_user_login_window", "dws_user_user_login_window"));
    }

    // 开窗聚合方法
    private SingleOutputStreamOperator<UserLoginBean> windowAndAgg(SingleOutputStreamOperator<UserLoginBean> beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserLoginBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(120L))
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<UserLoginBean>() {
                            @Override
                            public UserLoginBean reduce(UserLoginBean value1,
                                                        UserLoginBean value2) throws Exception {
                                value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                                value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                                return value1;
                            }
                        },
                        new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window,
                                              Iterable<UserLoginBean> values,
                                              Collector<UserLoginBean> out) throws Exception {
                                UserLoginBean bean = values.iterator().next();
                                bean.setStt(DateFormatUtil.tsToDateTime(window.getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(window.getEnd()));
                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(window.getStart()));
                                out.collect(bean);
                            }
                        }
                );
    }

    // 解析 JSON 对象为 UserLoginBean 的方法
    private SingleOutputStreamOperator<UserLoginBean> parseToPojo(SingleOutputStreamOperator<JSONObject> stream) {
        return stream
                .keyBy(obj -> obj.getJSONObject("common").getString("uid"))
                .process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {

                    private ValueState<String> lastLoginDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastLoginDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastLoginDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject obj,
                                               Context ctx,
                                               Collector<UserLoginBean> out) throws Exception {
                        Long ts = obj.getLong("ts");
                        String today = DateFormatUtil.tsToDate(ts);
                        String lastLoginDate = lastLoginDateState.value();
                        Long uuCt = 0L;
                        Long backCt = 0L;
                        if (!today.equals(lastLoginDate)) {
                            // 今天的第一次登录
                            uuCt = 1L;
                            lastLoginDateState.update(today);

                            // 计算回流: 曾经登录过
                            if (lastLoginDate != null) { //
                                long lastLoginTs = DateFormatUtil.dateToTs(lastLoginDate);

                                // 7日回流
                                if ((ts - lastLoginTs) / 1000 / 60 / 60 / 24 > 7) {
                                    backCt = 1L;
                                }
                            }
                        }
                        if (uuCt == 1) {
                            out.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                        }
                    }
                });
    }

    // 过滤出登录记录的方法
    private SingleOutputStreamOperator<JSONObject> filterLoginLog(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                    /*
                    登录日志:
                        自动登录
                            uid != null && last_page_id == null

                        手动登录
                            A -> 登录页面 -> B(登录成功)

                            udi != null && last_page_id = login

                        合并优化: uid != null && (last_page_id == null || last_page_id = login)
                     */
                        String uid = value.getJSONObject("common").getString("uid");
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return uid != null && (lastPageId == null || "login".equals(lastPageId));
                    }
                });
    }
}
