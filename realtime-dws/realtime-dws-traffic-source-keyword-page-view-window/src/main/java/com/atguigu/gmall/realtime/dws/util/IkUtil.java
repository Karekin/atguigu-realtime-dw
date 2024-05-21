package com.atguigu.gmall.realtime.dws.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

// 定义 IkUtil 工具类，用于分词处理
public class IkUtil {

    // 定义静态方法 split，用于对输入字符串进行分词
    public static Set<String> split(String s) {
        Set<String> result = new HashSet<>(); // 创建一个 Set 用于存储分词结果

        // 将输入字符串转换为 Reader 对象
        Reader reader = new StringReader(s);

        // 创建 IKSegmenter 对象，第二个参数为 true 表示使用智能分词模式
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        try {
            // 获取下一个词元
            Lexeme next = ikSegmenter.next();
            // 循环遍历词元
            while (next != null) {
                // 获取词元文本并添加到结果集合中
                String word = next.getLexemeText();
                result.add(word);
                // 获取下一个词元
                next = ikSegmenter.next();
            }
        } catch (IOException e) {
            // 捕获并抛出 IOException 异常
            throw new RuntimeException(e);
        }

        // 返回分词结果集合
        return result;
    }
}
