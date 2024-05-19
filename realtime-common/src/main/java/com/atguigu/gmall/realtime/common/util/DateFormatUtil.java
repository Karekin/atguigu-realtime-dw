package com.atguigu.gmall.realtime.common.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * 该工具类适用于各种需要进行日期时间格式转换的场景，特别是在大数据处理和实时数据流处理中。
 * 例如，可以将数据库中的日期时间字符串转换为时间戳进行存储和计算，或者将时间戳转换为特定格式的日期字符串用于展示和分区。
 */
public class DateFormatUtil {

    // 定义日期时间格式化器
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter dtfForPartition = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter dtfFull = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 将日期时间字符串转换为时间戳（毫秒值）
     * 日期时间格式：2023-07-05 01:01:01
     *
     * @param dateTime 日期时间字符串
     * @return 对应的时间戳（毫秒值）
     */
    public static Long dateTimeToTs(String dateTime) {
        // 解析日期时间字符串为 LocalDateTime 对象
        LocalDateTime localDateTime = LocalDateTime.parse(dateTime, dtfFull);
        // 转换为时间戳（毫秒值）
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    /**
     * 将时间戳（毫秒值）转换为日期字符串
     * 日期格式：2023-07-05
     *
     * @param ts 时间戳（毫秒值）
     * @return 对应的日期字符串
     */
    public static String tsToDate(Long ts) {
        // 创建 Date 对象
        Date dt = new Date(ts);
        // 转换为 LocalDateTime 对象
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        // 格式化为日期字符串
        return dtf.format(localDateTime);
    }

    /**
     * 将时间戳（毫秒值）转换为日期时间字符串
     * 日期时间格式：2023-07-05 01:01:01
     *
     * @param ts 时间戳（毫秒值）
     * @return 对应的日期时间字符串
     */
    public static String tsToDateTime(Long ts) {
        // 创建 Date 对象
        Date dt = new Date(ts);
        // 转换为 LocalDateTime 对象
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        // 格式化为日期时间字符串
        return dtfFull.format(localDateTime);
    }

    /**
     * 将时间戳（毫秒值）转换为分区日期字符串
     * 日期格式：20230705
     *
     * @param ts 时间戳（毫秒值）
     * @return 对应的分区日期字符串
     */
    public static String tsToDateForPartition(long ts) {
        // 创建 Date 对象
        Date dt = new Date(ts);
        // 转换为 LocalDateTime 对象
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        // 格式化为分区日期字符串
        return dtfForPartition.format(localDateTime);
    }

    /**
     * 将日期字符串转换为时间戳（毫秒值）
     * 日期格式：2023-07-05
     *
     * @param date 日期字符串
     * @return 对应的时间戳（毫秒值）
     */
    public static long dateToTs(String date) {
        // 调用 dateTimeToTs 方法，补全时间部分
        return dateTimeToTs(date + " 00:00:00");
    }
}
