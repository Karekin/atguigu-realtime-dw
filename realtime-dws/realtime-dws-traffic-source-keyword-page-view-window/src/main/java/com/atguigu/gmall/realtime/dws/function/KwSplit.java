package com.atguigu.gmall.realtime.dws.function;

import com.atguigu.gmall.realtime.dws.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Set;

/**
 * 作用：实现对输入的关键词字符串进行分词处理，并将分词后的关键词逐个输出。
 * 目标：通过分词技术将复合关键词拆分为独立的词语，便于后续的关键词统计和分析，提高数据处理的粒度和精度。
 */
@FunctionHint(output = @DataTypeHint("row<keyword string>")) // 指定输出数据类型
public class KwSplit extends TableFunction<Row> {
    // eval 方法是表函数的核心逻辑，用于接收输入并进行处理
    public void eval(String kw) {
        if (kw == null) {
            return; // 如果输入的关键词为null，则直接返回
        }
        // 使用 IkUtil 工具类对关键词进行分词
        Set<String> keywords = IkUtil.split(kw);
        // 遍历分词结果，并将每个关键词作为输出
        for (String keyword : keywords) {
            collect(Row.of(keyword)); // 将关键词封装成 Row 对象并输出
        }
    }
}
