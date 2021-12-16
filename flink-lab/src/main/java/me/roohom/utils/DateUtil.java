package me.roohom.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

/**
 * 日期工具类
 *
 * @author WY
 */
public class DateUtil {

    /**
     * 将时间戳转为时间字符串
     *
     * @param millis  毫秒时间戳
     * @param pattern 时间格式
     * @return 时间字符串
     */
    public static String millis2String(long millis, String pattern) {
        return new SimpleDateFormat(pattern, Locale.CHINA).format(new Date(millis));
    }

    /**
     * 日期格式字符串转时间戳
     *
     * @param date    日期字符串
     * @param pattern 时间格式
     * @return 毫秒时间戳
     */
    public static long string2millis(String date, String pattern) throws ParseException {
        return new SimpleDateFormat(pattern, Locale.CHINA).parse(date).getTime();
    }

    /**
     * 获取指定日期的前/后N天
     *
     * @param millis 毫秒时间戳
     * @param amount 负数表示前N天，正数表示后N天
     * @return 毫秒时间戳
     */

    public static long getLastOrNextNDay(long millis, int amount) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(millis);
        calendar.add(Calendar.DAY_OF_MONTH, amount);
        return calendar.getTimeInMillis();
    }
}
