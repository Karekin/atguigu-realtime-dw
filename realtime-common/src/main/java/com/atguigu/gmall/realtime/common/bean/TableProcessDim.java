package com.atguigu.gmall.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * TableProcessDim 类，用于表示数据处理过程中的维度表配置。
 * 包含来源表名、目标表名、输出字段、列族、主键字段和操作类型等信息。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDim {
    // 来源表名
    String sourceTable;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 数据到 HBase 的列族
    String sinkFamily;

    // sink 到 HBase 的时候的主键字段
    String sinkRowKey;

    // 配置表操作类型
    String op;
}
