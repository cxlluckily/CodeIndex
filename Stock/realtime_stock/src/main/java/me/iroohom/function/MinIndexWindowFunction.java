package me.iroohom.function;

import me.iroohom.bean.CleanBean;
import me.iroohom.bean.IndexBean;
import me.iroohom.constant.Constant;
import me.iroohom.util.DateUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.hsqldb.Index;

/**
 * @ClassName: MinIndexWindowFunction
 * @Author: Roohom
 * @Function: 分时窗口函数
 * @Date: 2020/11/15 21:36
 * @Software: IntelliJ IDEA
 */
public class MinIndexWindowFunction extends RichWindowFunction<CleanBean, IndexBean, String, TimeWindow> {
    MapState<String, IndexBean> indexMs;

    @Override
    public void open(Configuration parameters) throws Exception {
        indexMs = getRuntimeContext().getMapState(new MapStateDescriptor<String, IndexBean>("indexMs", String.class, IndexBean.class));
    }

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

        //设置分时成交额和成交量初始值
        Long curTradeAmt = 0L;
        Long curTradeVol = 0L;
        IndexBean indexBeanLast = indexMs.get(cleanBean.getSecCode());
        //获取到了indexBean 也就不是第一个窗口
        if (indexBeanLast != null) {
            // 第一个窗口则不会进入
            /**
             * 上一个窗口的分时成交金额
             */
            Long tradeAmtDayLast = indexBeanLast.getTradeAmtDay();
            /**
             * 上一个窗口的分时成交金额
             */
            Long tradeVolDayLast = indexBeanLast.getTradeVolDay();

            /**
             * 当前窗口的成交金额
             */
            Long tradeAmt = cleanBean.getTradeAmt();
            /**
             * 当前窗口的成交量
             */
            Long tradeVolume = cleanBean.getTradeVolume();

            /**
             * 分时成交量
             */
            curTradeVol = tradeVolume - tradeVolDayLast;
            curTradeAmt = tradeAmt - tradeAmtDayLast;
        }

        Long tradeTime = DateUtil.longTimeTransfer(cleanBean.getEventTime(), Constant.format_YYYYMMDDHHMMSS);
        IndexBean indexBean = new IndexBean(
                cleanBean.getEventTime(),
                cleanBean.getSecCode(),
                cleanBean.getSecName(),
                cleanBean.getPreClosePrice(),
                cleanBean.getOpenPrice(),
                cleanBean.getMaxPrice(),
                cleanBean.getMinPrice(),
                cleanBean.getTradePrice(),
                curTradeVol, curTradeAmt,
                cleanBean.getTradeVolume(),
                cleanBean.getTradeAmt(),
                tradeTime,
                cleanBean.getSource()
        );
        out.collect(indexBean);
        indexMs.put(cleanBean.getSecCode(), indexBean);
    }
}
