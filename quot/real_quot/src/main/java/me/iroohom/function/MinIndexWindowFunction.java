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
 * @Function: 指数分时窗口函数 获取指数分时行情数据（分时成交量/金额）
 * @Date: 2020/11/5 13:47
 * @Software: IntelliJ IDEA
 */
public class MinIndexWindowFunction extends RichWindowFunction<CleanBean, IndexBean, String, TimeWindow> {

    MapState<String, IndexBean> indexMs = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //缓存上一窗口的指数分时行情数据
        indexMs = getRuntimeContext().getMapState(new MapStateDescriptor<String, IndexBean>("indexMs", String.class, IndexBean.class));
    }

    /**
     * 开发步骤：
     * 1.新建MinIndexWindowFunction 窗口函数
     * 2.初始化 MapState<String, IndexBean>
     * 3.记录最新指数
     * 4.获取分时成交额和成交数量
     * 5.格式化日期
     * 6.封装输出数据
     * 7.更新MapState
     */
    @Override
    public void apply(String s, TimeWindow window, Iterable<CleanBean> input, Collector<IndexBean> out) throws Exception {
        //记录最新指数
        CleanBean cleanBean = null;
        for (CleanBean bean : input) {
            if (cleanBean == null) {
                cleanBean = bean;
            }
            if (cleanBean.getEventTime() < bean.getEventTime()) {
                cleanBean = bean;
            }
        }

        //获取分时成交额和成交数量
        Long curTradeVol = 0L;
        Long curTradeAmt = 0L;
        IndexBean indexBeanLast = indexMs.get(cleanBean.getSecCode());
        if (indexBeanLast != null) {
            //上一窗口的总成交金额
            Long tradeAmtDayLast = indexBeanLast.getTradeAmtDay();
            //上一窗口的总成交数量
            Long tradeVolDayLast = indexBeanLast.getTradeVolDay();

            //获取当前窗口的总成交金额和总成交量
            Long tradeAmt = cleanBean.getTradeAmt();
            Long tradeVolume = cleanBean.getTradeVolume();

            //计算分时成交量/金额
            curTradeVol = tradeVolume - tradeVolDayLast;
            curTradeAmt = tradeAmt - tradeAmtDayLast;
        }

        //格式化日期
        Long tradeTime = DateUtil.longTimeTransfer(cleanBean.getEventTime(), Constant.format_YYYYMMDDHHMMSS);

        //封装数据输出
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
                cleanBean.getSource());

        out.collect(indexBean);

        //更新状态
        indexMs.put(cleanBean.getSecCode(), indexBean);
    }
}
