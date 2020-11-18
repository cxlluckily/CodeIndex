package me.iroohom.function;

import me.iroohom.bean.CleanBean;
import me.iroohom.bean.IndexBean;
import me.iroohom.constant.Constant;
import me.iroohom.util.DateUtil;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName: SecIndexWindowFunction
 * @Author: Roohom
 * @Function: 秒级窗口数据处理
 * @Date: 2020/11/5 13:04
 * @Software: IntelliJ IDEA
 */
public class SecIndexWindowFunction extends RichWindowFunction<CleanBean, IndexBean, String, TimeWindow> {

    /**
     * 开发步骤：
     * 1.新建SecIndexWindowFunction 窗口函数
     * 2.记录最新指数
     * 3.格式化日期
     * 4.封装输出数据
     */
    @Override
    public void apply(String s, TimeWindow window, Iterable<CleanBean> input, Collector<IndexBean> out) throws Exception {
        CleanBean cleanBean = null;
        for (CleanBean bean : input) {
            if (cleanBean == null) {
                cleanBean = bean;
            }
            if (cleanBean.getEventTime() < bean.getEventTime()) {
                cleanBean = bean;
            }
        }

        //格式化日期
        Long tradeTime = DateUtil.longTimeTransfer(cleanBean.getEventTime(), Constant.format_YYYYMMDDHHMMSS);
        //4.封装输出数据
        //eventTime、indexCode、indexName、preClosePrice、openPrice、highPrice、lowPrice、closePrice、
        //tradeVol、tradeAmt、tradeVolDay、tradeAmtDay、tradeTime、source
        out.collect(new IndexBean(
                cleanBean.getEventTime(),
                cleanBean.getSecCode(),
                cleanBean.getSecName(),
                cleanBean.getPreClosePrice(),
                cleanBean.getOpenPrice(),
                cleanBean.getMaxPrice(),
                cleanBean.getMinPrice(),
                cleanBean.getTradePrice(),
                0L,0L,
                cleanBean.getTradeVolume(),
                cleanBean.getTradeAmt(),
                tradeTime,
                cleanBean.getSource()
        ));
    }
}
