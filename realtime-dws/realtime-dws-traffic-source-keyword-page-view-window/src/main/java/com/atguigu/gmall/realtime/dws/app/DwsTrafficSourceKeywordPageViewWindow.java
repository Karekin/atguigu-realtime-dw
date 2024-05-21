package com.atguigu.gmall.realtime.dws.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import com.atguigu.gmall.realtime.dws.function.KwSplit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 作用：实时统计不同来源的搜索关键词的页面浏览量，并将统计结果存储到Doris数据库中，供后续数据分析使用。
 * 目标：通过实时流处理技术，实现对搜索关键词的实时监控和分析，及时获取用户搜索行为数据，提升数据处理的实时性和准确性。
 */
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    // main方法是程序的入口，设置了应用的参数并启动应用
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(
                10021,  // 应用ID
                4,      // 并行度
                "dws_traffic_source_keyword_page_view_window" // 应用名称
        );
    }

    // 重写handle方法，用于处理流数据
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 读取页面日志
        tEnv.executeSql("create table page_log(" +
                " page map<string, string>, " + // 页面信息，使用Map类型存储
                " ts bigint, " +  // 时间戳
                " et as to_timestamp_ltz(ts, 3), " +  // 将时间戳转换为时间类型
                " watermark for et as et - interval '5' second " +  // 设置水印，允许最大5秒的延迟
                ")" + SQLUtil.getKafkaDDLSource("dws_traffic_source_keyword_page_view_window", Constant.TOPIC_DWD_TRAFFIC_PAGE)); // Kafka源表配置

        // 2. 读取搜索关键词
        Table kwTable = tEnv.sqlQuery("select " +
                "page['item'] kw, " +  // 获取关键词
                "et " +  // 获取时间
                "from page_log " +
                "where ( page['last_page_id'] ='search' " +  // 过滤条件，前一个页面是搜索页或首页
                "        or page['last_page_id'] ='home' " +
                "       )" +
                "and page['item_type']='keyword' " +  // 关键词类型
                "and page['item'] is not null ");  // 关键词不为空
        tEnv.createTemporaryView("kw_table", kwTable); // 创建临时视图

        // 3. 自定义分词函数
        tEnv.createTemporaryFunction("kw_split", KwSplit.class); // 注册自定义分词函数

        // 使用自定义分词函数对关键词进行分词
        Table keywordTable = tEnv.sqlQuery("select " +
                " keyword, " +  // 分词后的关键词
                " et " +  // 时间
                "from kw_table " +
                "join lateral table(kw_split(kw)) on true "); // 关联自定义分词函数
        tEnv.createTemporaryView("keyword_table", keywordTable); // 创建临时视图

        // 4. 开窗聚合
        Table result = tEnv.sqlQuery("select " +
                " date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt, " +  // 窗口开始时间
                " date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt, " +  // 窗口结束时间
                " date_format(window_start, 'yyyyMMdd') cur_date, " +  // 当前日期
                " keyword," +  // 关键词
                " count(*) keyword_count " +  // 关键词计数
                "from table( tumble(table keyword_table, descriptor(et), interval '5' second ) ) " +  // 定义滚动窗口，窗口大小为5秒
                "group by window_start, window_end, keyword "); // 按窗口和关键词分组

        // 5. 写出到Doris中
        tEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(" +
                "  stt string, " +  // 窗口开始时间字符串
                "  edt string, " +  // 窗口结束时间字符串
                "  cur_date string, " +  // 当前日期字符串
                "  keyword string, " +  // 关键词
                "  keyword_count bigint " +  // 关键词计数
                ")with(" +
                " 'connector' = 'doris'," +  // Doris连接器
                " 'fenodes' = '" + Constant.DORIS_FE_NODES + "'," +  // Doris前端节点
                "  'table.identifier' = '" + Constant.DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window'," +  // 表标识符
                "  'username' = 'root'," +  // 用户名
                "  'password' = '000000', " +  // 密码
                "  'sink.properties.format' = 'json', " +  // 输出格式为JSON
                "  'sink.buffer-count' = '4', " +  // 缓冲区计数
                "  'sink.buffer-size' = '4086'," +  // 缓冲区大小
                "  'sink.enable-2pc' = 'false', " +  // 关闭两阶段提交
                "  'sink.properties.read_json_by_line' = 'true' " +  // 按行读取JSON
                ")");
        result.executeInsert("dws_traffic_source_keyword_page_view_window"); // 执行插入操作，将结果写入Doris
    }
}
