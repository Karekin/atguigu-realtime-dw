package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;

/**
 * DimFunction接口，定义了从维度表中获取和处理数据的方法。
 *
 * @param <T> 泛型，表示处理的bean类型
 */
public interface DimFunction<T> {

    /**
     * 获取bean对应的行键（row key）
     *
     * @param bean 处理的bean对象
     * @return 行键字符串
     */
    String getRowKey(T bean);

    /**
     * 获取维度表的表名
     *
     * @return 维度表的表名
     */
    String getTableName();

    /**
     * 将维度数据添加到bean中
     *
     * @param bean 处理的bean对象
     * @param dim  从维度表中获取的维度数据
     */
    void addDims(T bean, JSONObject dim);
}
