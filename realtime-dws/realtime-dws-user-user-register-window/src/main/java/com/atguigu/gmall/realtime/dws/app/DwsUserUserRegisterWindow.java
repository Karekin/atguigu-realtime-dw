package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.UserRegisterBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 这段代码是一个基于 Apache Flink 的实时数据处理应用，
 * 主要用于处理用户注册数据，计算每隔一段时间内的新注册用户数。具体流程如下：
     * 从 Kafka 读取用户注册日志。
     * 解析 JSON 数据，并提取事件时间戳。
     * 按照事件时间进行窗口聚合计算，统计每个窗口内的注册用户数量。
     * 将计算结果封装到 UserRegisterBean 对象中。
     * 将结果写入 Doris 数据库中。
 */
public class DwsUserUserRegisterWindow extends BaseApp {
    // main 方法是程序的入口
    public static void main(String[] args) {
        new DwsUserUserRegisterWindow().start(
                10025, // Flink 任务的并行度
                4, // 检查点的保存间隔
                "dws_user_user_register_window", // 任务名称
                Constant.TOPIC_DWD_USER_REGISTER // Kafka 主题
        );
    }

    // handle 方法是处理数据的主流程
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 解析 JSON 数据，并提取事件时间戳
        stream
                .map(JSON::parseObject) // 将字符串转换为 JSON 对象
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L)) // 设置允许的最大乱序时间为 5 秒
                                .withTimestampAssigner((obj, ts) -> obj.getLong("create_time")) // 提取事件时间戳
                                .withIdleness(Duration.ofSeconds(120L)) // 设置空闲超时时间为 120 秒
                )
                // 2. 按照事件时间进行滚动窗口聚合计算
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L))) // 设置滚动窗口的大小为 5 秒
                .aggregate(
                        // 使用 AggregateFunction 进行增量聚合
                        new AggregateFunction<JSONObject, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L; // 初始化累加器
                            }

                            @Override
                            public Long add(JSONObject value, Long acc) {
                                return acc + 1; // 每接收到一个注册事件，累加器加 1
                            }

                            @Override
                            public Long getResult(Long acc) {
                                return acc; // 返回累加结果
                            }

                            @Override
                            public Long merge(Long acc1, Long acc2) {
                                return acc1 + acc2; // 合并两个累加器
                            }
                        },
                        // 使用 ProcessAllWindowFunction 处理聚合结果
                        new ProcessAllWindowFunction<Long, UserRegisterBean, TimeWindow>() {
                            @Override
                            public void process(Context ctx,
                                                Iterable<Long> elements,
                                                Collector<UserRegisterBean> out) throws Exception {
                                Long result = elements.iterator().next(); // 获取聚合结果

                                // 封装聚合结果到 UserRegisterBean 对象中
                                out.collect(new UserRegisterBean(
                                        DateFormatUtil.tsToDateTime(ctx.window().getStart()), // 窗口开始时间
                                        DateFormatUtil.tsToDateTime(ctx.window().getEnd()), // 窗口结束时间
                                        DateFormatUtil.tsToDateForPartition(ctx.window().getEnd()), // 分区时间
                                        result // 注册用户数量
                                ));
                            }
                        }
                )
                // 3. 将结果写入 Doris 数据库
                .map(new DorisMapFunction<>()) // 将 UserRegisterBean 对象映射为 Doris 可以接受的数据格式
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABASE + ".dws_user_user_register_window", "dws_user_user_register_window")); // 将数据写入 Doris 数据库
    }
}
