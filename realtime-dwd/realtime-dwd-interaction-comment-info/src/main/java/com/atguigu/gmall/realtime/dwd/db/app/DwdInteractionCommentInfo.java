package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * DwdInteractionCommentInfo 继承自 BaseSQLApp，用于处理评论信息的实时数据流，
 * 包含数据过滤、维度表关联、结果写入 Kafka 等步骤，确保数据处理的实时性和准确性。
 */
public class DwdInteractionCommentInfo extends BaseSQLApp {

    // 主方法，程序入口
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(
                10012,  // 应用程序的端口号
                4,      // 并行度
                Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO  // Kafka 主题名称
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        // 1. 通过 DDL 的方式建立动态表，从 Kafka 主题读取数据
        readOdsDb(tEnv, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

        // 2. 过滤出评论表的数据
        Table commentInfo = tEnv.sqlQuery(
                "select " +
                        " `data`['id'] id, " +
                        " `data`['user_id'] user_id, " +
                        " `data`['sku_id'] sku_id, " +
                        " `data`['appraise'] appraise, " +
                        " `data`['comment_txt'] comment_txt, " +
                        " `data`['create_time'] comment_time," +
                        " ts, " +
                        " pt " +
                        " from topic_db " +
                        " where `database`='gmall' " +
                        " and `table`='comment_info' " +
                        " and `type`='insert' ");  // 过滤条件
        tEnv.createTemporaryView("comment_info", commentInfo);  // 创建临时视图

        // 3. 通过 DDL 的方式建表：从 HBase 读取维度表 base_dic
        readBaseDic(tEnv);

        // 4. 将事实表与维度表进行 Join 操作，使用 Lookup Join
        Table result = tEnv.sqlQuery("select " +
                "ci.id, " +
                "ci.user_id," +
                "ci.sku_id," +
                "ci.appraise," +
                "dic.info.dic_name appraise_name," +
                "ci.comment_txt," +
                "ci.ts " +
                "from comment_info ci " +
                "join base_dic for system_time as of ci.pt as dic " +
                "on ci.appraise=dic.dic_code");

        // 5. 通过 DDL 的方式建立 Sink 表，用于将结果写入 Kafka
        tEnv.executeSql("create table dwd_interaction_comment_info(" +
                "id string, " +
                "user_id string," +
                "sku_id string," +
                "appraise string," +
                "appraise_name string," +
                "comment_txt string," +
                "ts bigint " +
                ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));

        // 6. 将 Join 的结果写入 Sink 表（Kafka 主题）
        result.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
}
