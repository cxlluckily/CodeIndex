package me.iroohom.function;

import me.iroohom.bean.CleanBean;
import me.iroohom.bean.StockBean;
import me.iroohom.constant.Constant;
import me.iroohom.util.DateUtil;
import me.iroohom.util.DbUtil;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName: SecStockWindowFunction
 * @Author: Roohom
 * @Function: Hbase秒级窗口处理函数 每5秒取最新的一条数据 （因为页面查询不需要频繁查询 页面每5秒刷新一次就查询一次）
 * @Date: 2020/11/1 20:10
 * @Software: IntelliJ IDEA
 */
public class SecStockWindowFunction implements WindowFunction<CleanBean, StockBean, String, TimeWindow> {

    /**
     * 开发步骤：
     * 2.记录最新个股
     * 3.格式化日期
     * 4.封装输出数据
     *
     * @param s      分组的str
     * @param window 窗口
     * @param input  输入数据
     * @param out    输出数据
     * @throws Exception
     */

    @Override
    public void apply(String s, TimeWindow window, Iterable<CleanBean> input, Collector<StockBean> out) throws Exception {

        //用于记录最新个股的CleanBean记录最新个股
        CleanBean cleanBean = null;
        for (CleanBean bean : input) {
            //如果最新个股为null，就设置为遍历输入数据得到的bean
            if (cleanBean == null) {
                cleanBean = bean;
            }
            //如果最新个股的事件时间小于bean的事件时间，就将该bean替换最新个股作为最新个股
            if (cleanBean.getEventTime() < bean.getEventTime()) {
                cleanBean = bean;
            }
        }

        //格式化日期
        Long tradeTime = DateUtil.longTimeTransfer(cleanBean.getEventTime(), Constant.format_YYYYMMDDHHMMSS);

        //4.封装输出数据
        //  //eventTime、secCode、secName、preClosePrice、openPrice、highPrice、lowPrice、closePrice、
        //    //tradeVol、tradeAmt、tradeVolDay、tradeAmtDay、tradeTime、source
        StockBean stockBean = new StockBean(
                cleanBean.getEventTime(),
                cleanBean.getSecCode(),
                cleanBean.getSecName(),
                cleanBean.getPreClosePrice(),
                cleanBean.getOpenPrice(),
                cleanBean.getMaxPrice(),
                cleanBean.getMinPrice(),
                cleanBean.getTradePrice(),
                0L, 0L,
                cleanBean.getTradeVolume(),
                cleanBean.getTradeAmt(),
                tradeTime,
                cleanBean.getSource());

        //收集数据
        out.collect(stockBean);
    }
}
