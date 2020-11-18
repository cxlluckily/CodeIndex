package me.iroohom.util;

import me.iroohom.config.QuotConfig;

import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

/**
 * @author roohoo
 * @Date 2020/10/29
 */
public class SpecialTimeUtil {

    /**
     * 开发步骤：
     * 1.获取配置文件日期数据
     * 2.新建Calendar对象，设置日期
     * 3.设置开市时间
     * 4.获取开市时间
     * 5.设置闭市时间
     * 6.获取闭市时间
     */
    static long openTime = 0L;
    static long closeTime = 0L;
    static {
        //1.获取配置文件日期数据
        Properties properties  = QuotConfig.config;
        String date = properties.getProperty("date");
        String beginTime  = properties.getProperty("open.time");
        String endTime = properties.getProperty("close.time");

        //2.新建Calendar对象，设置日期
        Calendar calendar = Calendar.getInstance();

        if (date == null) {
            calendar.setTime(new Date());
        } else {
            calendar.setTimeInMillis(DateUtil.stringToLong(date, "yyyyMMdd"));
        }

        //3.设置开市时间
        if (beginTime == null) {
            calendar.set(Calendar.HOUR_OF_DAY, 9);
            calendar.set(Calendar.MINUTE, 30);
        } else {
            String[] arr = beginTime.split(":");
            calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(arr[0]));
            calendar.set(Calendar.MINUTE, Integer.parseInt(arr[1]));
        }

        //设置秒
        calendar.set(Calendar.SECOND, 0);
        //设置毫秒
        calendar.set(Calendar.MILLISECOND, 0);

        //4.获取开市时间
        openTime = calendar.getTime().getTime();

        //5.设置闭市时间
        if (endTime == null) {
            calendar.set(Calendar.HOUR_OF_DAY, 15);
            calendar.set(Calendar.MINUTE, 0);
        } else {
            String[] arr = endTime.split(":");
            calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(arr[0]));
            calendar.set(Calendar.MINUTE, Integer.parseInt(arr[1]));
        }

        //6获取闭市时间
        closeTime = calendar.getTime().getTime();
    }

    public static void main(String[] args) {
        System.out.println(openTime);
        System.out.println(closeTime);
    }

}
