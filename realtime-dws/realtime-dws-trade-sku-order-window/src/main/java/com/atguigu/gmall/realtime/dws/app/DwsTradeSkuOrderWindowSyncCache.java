package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.dws.function.MapDimFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
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

import java.math.BigDecimal;
import java.time.Duration;

/**
 * 这个 Java 代码实现了一个基于 Apache Flink 的实时数据处理应用程序。
 * 其主要目的是从 Kafka 中读取订单明细数据，去重并聚合这些数据，然后补充商品维度信息。
 * 最后，处理后的数据会进行输出。本应用程序的具体流程包括：
     * 读取 Kafka 中的订单明细数据并解析成 Java 对象。
     * 去重操作，以确保每个订单详情记录唯一。
     * 按照 SKU（库存单位）进行分组、开窗和聚合操作。
     * 根据不同的维度表（如 SKU 信息、SPU 信息、商标信息、分类信息等）补充商品维度信息。
 */
public class DwsTradeSkuOrderWindowSyncCache extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindowSyncCache().start(
                60009,  // 启动端口
                1,      // 并行度
                "dws_trade_sku_order_window",  // 作业名称
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL  // Kafka 主题
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 解析成 pojo 类型
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = parseToPojo(stream);

        // 2. 按照 order_detail_id 去重
        beanStream = distinctByOrderId(beanStream);

        // 3. 按照 sku_id 分组, 开窗, 聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStreamWithoutDims = windowAndAgg(beanStream);

        // 4. join 维度
        joinDim(beanStreamWithoutDims);
    }

    private void joinDim(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        /*
            每来一条数据, 都需要去维度层(hbase)中查找对应的维度, 补充到 bean 中

            6 张维度表:
                dim_sku_info  sku_id
                    sku_name spu_id tm_id c3_id
                dim_spu_info spu_id
                    spu_name
                dim_base_trademark tm_id
                    tm_name
                dim_base_category3  c3_id
                    c3_name c2_id
                dim_base_category2 c2_id
                    c2_name c1_id
                dim_base_category3 c1_id
                    c1_name
         */
        // 补充 sku 信息
        SingleOutputStreamOperator<TradeSkuOrderBean> skuSteam = stream
                .map(new MapDimFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getTableName() {
                        return "dim_sku_info";  // 获取 SKU 信息表名
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getSkuId();  // 获取 SKU ID 作为行键
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setSkuName(dim.getString("sku_name"));  // 设置 SKU 名称
                        bean.setSpuId(dim.getString("spu_id"));  // 设置 SPU ID
                        bean.setTrademarkId(dim.getString("tm_id"));  // 设置商标 ID
                        bean.setCategory3Id(dim.getString("category3_id"));  // 设置三级分类 ID
                    }
                });

        // 补充 spu 信息
        SingleOutputStreamOperator<TradeSkuOrderBean> spuStream = skuSteam.map(new MapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getSpuId();  // 获取 SPU ID 作为行键
            }

            @Override
            public String getTableName() {
                return "dim_spu_info";  // 获取 SPU 信息表名
            }

            @Override
            public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                bean.setSpuName(dim.getString("spu_name"));  // 设置 SPU 名称
            }
        });

        // 补充商标信息
        SingleOutputStreamOperator<TradeSkuOrderBean> tmStream = spuStream
                .map(new MapDimFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getTrademarkId();  // 获取商标 ID 作为行键
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";  // 获取商标信息表名
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setTrademarkName(dim.getString("tm_name"));  // 设置商标名称
                    }
                });

        // 补充三级分类信息
        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = tmStream.map(new MapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getCategory3Id();  // 获取三级分类 ID 作为行键
            }

            @Override
            public String getTableName() {
                return "dim_base_category3";  // 获取三级分类信息表名
            }

            @Override
            public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                bean.setCategory3Name(dim.getString("name"));  // 设置三级分类名称
                bean.setCategory2Id(dim.getString("category2_id"));  // 设置二级分类 ID
            }
        });

        // 补充二级分类信息
        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = c3Stream.map(new MapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getCategory2Id();  // 获取二级分类 ID 作为行键
            }

            @Override
            public String getTableName() {
                return "dim_base_category2";  // 获取二级分类信息表名
            }

            @Override
            public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                bean.setCategory2Name(dim.getString("name"));  // 设置二级分类名称
                bean.setCategory1Id(dim.getString("category1_id"));  // 设置一级分类 ID
            }
        });

        // 补充一级分类信息
        SingleOutputStreamOperator<TradeSkuOrderBean> c1Stream = c2Stream.map(new MapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getCategory1Id();  // 获取一级分类 ID 作为行键
            }

            @Override
            public String getTableName() {
                return "dim_base_category1";  // 获取一级分类信息表名
            }

            @Override
            public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                bean.setCategory1Name(dim.getString("name"));  // 设置一级分类名称
            }
        });

        c1Stream.print();  // 打印最终结果
    }

    /**
     * 该方法实现了对数据流中订单数据的分组、开窗和聚合，并在窗口结束时输出聚合后的结果
     * @param beanStream
     * @return
     */
    private SingleOutputStreamOperator<TradeSkuOrderBean> windowAndAgg(SingleOutputStreamOperator<TradeSkuOrderBean> beanStream) {
        return beanStream
                // 分配时间戳和生成水印，用于处理乱序数据
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                // 指定从 TradeSkuOrderBean 对象的 ts 字段提取时间戳
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                // 设置水印生成的最大空闲时间
                                .withIdleness(Duration.ofSeconds(120))
                )
                // 按 skuId 进行分组
                .keyBy(TradeSkuOrderBean::getSkuId)
                // 设置滚动窗口，窗口大小为 5 秒
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)))
                // 在窗口中进行聚合操作
                .reduce(
                        // ReduceFunction 实现聚合逻辑
                        new ReduceFunction<TradeSkuOrderBean>() {
                            @Override
                            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                                // 将两个 TradeSkuOrderBean 对象的金额字段进行累加
                                value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                                value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                                value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                                // 返回累加后的结果
                                return value1;
                            }
                        },
                        // ProcessWindowFunction 实现窗口处理逻辑
                        new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String skuId, Context ctx, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) throws Exception {
                                // 获取当前窗口中的第一个元素
                                TradeSkuOrderBean bean = elements.iterator().next();
                                // 设置窗口开始时间
                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                // 设置窗口结束时间
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                                // 设置当前日期，用于分区
                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getEnd()));
                                // 输出结果
                                out.collect(bean);
                            }
                        }
                );
    }


    private SingleOutputStreamOperator<TradeSkuOrderBean> distinctByOrderId(SingleOutputStreamOperator<TradeSkuOrderBean> beanStream) {
        /*
        去重逻辑: 按照 order_detail_id 分组
            思路: 1
                使用 session 窗口, 当窗口关闭的时候, 同一个详情的多条数据,一定都来齐了.找到最完整的那个.
                    需要 dwd 层的数据添加一个字段,表示这条数据的生成时间, 时间大的那个就是最完整的
                        详情id  sku_id  金额   活动      优惠券  系统时间
                          1      100    100  null      null     1
                          null
                          1     100   100   有值      null       2
                          1     100   100   有值      有值        3

                   优点: 简单
                   缺点: 实效性低
                        窗口内的最后一条数据到了之后,经过一个 gap(5s)才能计算出结果

             思路: 2
                定时器
                    当第一条数据到的时候, 注册一个 5s 后触发的定时器, 把这条存入到状态中
                    每来一条数据, 就和状态中的比较时间, 时间大的保存到状态中.
                    当定时器触发的时候, 则状态中存储的一定是时间最大的那个

                    优点: 简单
                    确定: 实效性低
                        窗口内的第一条数据到了之后,5s才能计算出结果

             思路: 3  抵消
                            详情id  sku_id  左表金额  右表1活动    右表2优惠券
                   第一条     1      100    100       null      null       直接输出
                             1      100    -100      null      null       直接输出
                   第二条     1      100    100       200      null        直接输出
                             1      100     -100      -200      null      直接输出
                   第三条     1      100    100       200      300         直接输出
                优点: 实效性高
                缺点: 写放大

                    优化:
                            详情id  sku_id    左表金额  右表1活动    右表2优惠券
                   第一条     1      100      100       null      null       直接输出
                   第二条     1      100     100+(-100)  200      null        直接输出
                   第三条     1      100    100+(-100) 200+(-200)      300    直接输出
                 优点: 实效性高

               思路 4:
                    如果需要聚合的值都在左表, 不需要等最后最完整的数据.

                    只需要输出第一条就行了.

                    优点: 实效性最高
                    缺点: 特殊情况. 聚和的值都在左表
         */
        return beanStream
                .keyBy(TradeSkuOrderBean::getOrderDetailId)  // 按订单详情 ID 分组
                .process(new KeyedProcessFunction<String, TradeSkuOrderBean, TradeSkuOrderBean>() {

                    private ValueState<TradeSkuOrderBean> lastBeanState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<TradeSkuOrderBean> des = new ValueStateDescriptor<>("lastBean", TradeSkuOrderBean.class);
                        StateTtlConfig conf = new StateTtlConfig.Builder(Time.seconds(60))
                                .useProcessingTime()
                                .updateTtlOnCreateAndWrite()
                                .neverReturnExpired()
                                .build();
                        des.enableTimeToLive(conf);
                        lastBeanState = getRuntimeContext().getState(des);  // 初始化状态
                    }

                    @Override
                    public void processElement(TradeSkuOrderBean currentBean, Context ctx, Collector<TradeSkuOrderBean> out) throws Exception {
                        TradeSkuOrderBean lastBean = lastBeanState.value();
                        if (lastBean == null) { // 第1条数据来了, 则直接输出
                            out.collect(currentBean);
                        } else {
                            // 把上条数据的值取反, 然后和当前的 bean 中的值相加, 输出计算后的值
                            lastBean.setOrderAmount(currentBean.getOrderAmount().subtract(lastBean.getOrderAmount()));
                            lastBean.setOriginalAmount(currentBean.getOriginalAmount().subtract(lastBean.getOriginalAmount()));
                            lastBean.setActivityReduceAmount(currentBean.getActivityReduceAmount().subtract(lastBean.getActivityReduceAmount()));
                            lastBean.setCouponReduceAmount(currentBean.getCouponReduceAmount().subtract(lastBean.getCouponReduceAmount()));
                            out.collect(lastBean);
                        }

                        // 把当前的 bean 更新到状态中
                        lastBeanState.update(currentBean);
                    }
                });
    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(new MapFunction<String, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(String value) throws Exception {
                        JSONObject obj = JSON.parseObject(value);

                        return TradeSkuOrderBean.builder()
                                .skuId(obj.getString("sku_id"))  // 设置 SKU ID
                                .orderDetailId(obj.getString("id"))  // 设置订单详情 ID
                                .originalAmount(obj.getBigDecimal("split_original_amount"))  // 设置原始金额
                                .orderAmount(obj.getBigDecimal("split_total_amount"))  // 设置订单金额
                                .activityReduceAmount(obj.getBigDecimal("split_activity_amount") == null ? new BigDecimal("0.0") : obj.getBigDecimal("split_activity_amount"))  // 设置活动减免金额
                                .couponReduceAmount(obj.getBigDecimal("split_coupon_amount") == null ? new BigDecimal("0.0") : obj.getBigDecimal("split_coupon_amount"))  // 设置优惠券减免金额
                                .ts(obj.getLong("ts") * 1000)  // 设置时间戳
                                .build();
                    }
                });
    }
}
