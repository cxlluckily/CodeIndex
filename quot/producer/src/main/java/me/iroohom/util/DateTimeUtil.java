package me.iroohom.util;

import java.util.Calendar;

/**
 * @ClassName: DateTimeUtil
 * @Author: Roohom
 * @Function: 时间工具类，用于得到开盘时间和收盘时间
 * @Date: 2020/10/29 10:42
 * @Software: IntelliJ IDEA
 */
public class DateTimeUtil {
    public static long openTime = 0L;
    public static long closeTime = 0L;

    static {
        Calendar calendar = Calendar.getInstance();
        //设置开市时间，24小时制的9点，不用12小时制的9点
        calendar.set(Calendar.HOUR_OF_DAY, 9);
        calendar.set(Calendar.MINUTE, 30);
        calendar.set(Calendar.SECOND, 0);
        openTime = calendar.getTime().getTime();

        //设置闭市时间，应该是设置未到的时间
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        closeTime = calendar.getTime().getTime();

    }


    public static void main(String[] args) {
        System.out.println(openTime);
        System.out.println(closeTime);
    }


}
