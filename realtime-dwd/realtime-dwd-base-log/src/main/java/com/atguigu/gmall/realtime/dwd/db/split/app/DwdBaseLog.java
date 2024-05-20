package com.atguigu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * 这段代码通过 Flink 实现了对日志数据的实时处理，包括数据清洗、用户状态验证和日志分流，
 * 并将处理后的数据写入到 Kafka 中。每个步骤都进行了详细的处理，确保数据的准确性和及时性。

 * 数据清洗 (ETL)：过滤并解析输入的日志数据。
     * 验证新老客户状态：根据用户的访问时间，更新用户的新老状态。
     * 分流：将不同类型的日志（启动日志、页面日志、曝光日志、活动日志、错误日志）分流到不同的流。
     * 写入 Kafka：将分流后的数据写入到对应的 Kafka 主题中。
 */
@Slf4j
public class DwdBaseLog extends BaseApp {

    private final String START = "start";  // 启动日志标识
    private final String ERR = "err";      // 错误日志标识
    private final String DISPLAY = "display";  // 曝光日志标识
    private final String ACTION = "action";    // 活动日志标识
    private final String PAGE = "page";        // 页面日志标识

    // 主方法，启动应用程序
    public static void main(String[] args) {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 数据清洗
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);
        // 2. 验证新老客户状态
        SingleOutputStreamOperator<JSONObject> validatedStream = validateNewOrOld(etledStream);
        // 3. 分流
        Map<String, DataStream<JSONObject>> streams = splitStream(validatedStream);
        // 4. 写入 Kafka
        writeToKafka(streams);
    }

    // 将不同类型的日志写入到不同的 Kafka 主题中
    private void writeToKafka(Map<String, DataStream<JSONObject>> streams) {
        streams.get(START).map(JSONAware::toJSONString).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        streams.get(ERR).map(JSONAware::toJSONString).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        streams.get(DISPLAY).map(JSONAware::toJSONString).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        streams.get(PAGE).map(JSONAware::toJSONString).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        streams.get(ACTION).map(JSONAware::toJSONString).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }

    // 分流，将不同类型的日志分流到不同的 DataStream
    private Map<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> stream) {
        // 创建侧输出流标签
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display"){};
        OutputTag<JSONObject> actionTag = new OutputTag<JSONObject>("action"){};
        OutputTag<JSONObject> errTag = new OutputTag<JSONObject>("err"){};
        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>("page"){};

        SingleOutputStreamOperator<JSONObject> startStream = stream.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject obj, Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject common = obj.getJSONObject("common");
                Long ts = obj.getLong("ts");

                // 1. 启动日志
                JSONObject start = obj.getJSONObject("start");
                if (start != null) {
                    out.collect(obj);
                }

                // 2. 曝光日志
                JSONArray displays = obj.getJSONArray("displays");
                if (displays != null) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);
                        display.putAll(common);
                        display.put("ts", ts);
                        ctx.output(displayTag, display);
                    }
                    obj.remove("displays");
                }

                // 3. 活动日志
                JSONArray actions = obj.getJSONArray("actions");
                if (actions != null) {
                    for (int i = 0; i < actions.size(); i++) {
                        JSONObject action = actions.getJSONObject(i);
                        action.putAll(common);
                        ctx.output(actionTag, action);
                    }
                    obj.remove("actions");
                }

                // 4. 错误日志
                JSONObject err = obj.getJSONObject("err");
                if (err != null) {
                    ctx.output(errTag, obj);
                    obj.remove("err");
                }

                // 5. 页面日志
                JSONObject page = obj.getJSONObject("page");
                if (page != null) {
                    ctx.output(pageTag, obj);
                }
            }
        });

        Map<String, DataStream<JSONObject>> streams = new HashMap<>();
        streams.put(START, startStream);
        streams.put(DISPLAY, startStream.getSideOutput(displayTag));
        streams.put(ERR, startStream.getSideOutput(errTag));
        streams.put(PAGE, startStream.getSideOutput(pageTag));
        streams.put(ACTION, startStream.getSideOutput(actionTag));

        return streams;
    }

    // 验证新老客户状态的方法，输入为处理后的 JSON 数据流，输出为带有新老客户状态的 JSON 数据流
    private SingleOutputStreamOperator<JSONObject> validateNewOrOld(SingleOutputStreamOperator<JSONObject> stream) {
        // 将数据按 "mid" 键值进行分区，"mid" 是用户的唯一标识符
        return stream.keyBy(obj -> obj.getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    // 定义一个状态变量，用于存储用户的首次访问日期
                    private ValueState<String> firstVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化状态变量
                        firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject obj, Context ctx, Collector<JSONObject> out) throws Exception {
                        // 获取日志中的公共字段
                        JSONObject common = obj.getJSONObject("common");
                        // 获取用户的新老状态
                        String isNew = common.getString("is_new");

                        // 获取日志的时间戳
                        Long ts = obj.getLong("ts");
                        // 将时间戳转换为日期格式
                        String today = DateFormatUtil.tsToDate(ts);

                        // 从状态中获取首次访问日期
                        String firstVisitDate = firstVisitDateState.value();

                        // 如果用户是新用户
                        if ("1".equals(isNew)) {
                            if (firstVisitDate == null) {
                                // 如果状态中没有首次访问日期，说明是首次访问，更新状态为今天
                                firstVisitDateState.update(today);
                            } else if (!today.equals(firstVisitDate)) {
                                // 如果今天和首次访问日期不一致，说明数据有误，将新用户标识修改为老用户
                                common.put("is_new", "0");
                            }
                        } else {
                            // 如果用户是老用户
                            if (firstVisitDate == null) {
                                // 如果状态中没有首次访问日期，说明数据有误，将首次访问日期设置为前一天
                                firstVisitDateState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000));
                            }
                        }
                        // 输出处理后的数据
                        out.collect(obj);
                    }
                });
    }


    // 数据清洗
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                try {
                    JSON.parseObject(value);
                    return true;
                } catch (Exception e) {
                    log.error("日志格式不是正确的 JSON 格式: " + value);
                    return false;
                }
            }
        }).map(JSON::parseObject);
    }
}

