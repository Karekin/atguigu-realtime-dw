package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * DorisMapFunction类，用于将对象序列化为符合Doris数据库要求的JSON字符串。
 *
 * @param <T> 泛型，表示需要处理的对象类型
 */
public class DorisMapFunction<T> implements MapFunction<T, String> {

    /**
     * 将传入的对象序列化为JSON字符串，并将属性名转换为蛇形命名格式。
     *
     * @param bean 需要处理的对象
     * @return 序列化后的JSON字符串
     * @throws Exception 序列化过程中可能抛出的异常
     */
    @Override
    public String map(T bean) throws Exception {
        // 创建SerializeConfig对象，用于配置序列化策略
        SerializeConfig conf = new SerializeConfig();
        // 设置属性命名策略为蛇形命名
        conf.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        // 将对象序列化为JSON字符串，使用指定的命名策略
        return JSON.toJSONString(bean, conf);
    }
}
