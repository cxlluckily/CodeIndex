package me.iroohom.function;

import me.iroohom.bean.CleanBean;
import me.iroohom.bean.StockIncrBean;
import me.iroohom.constant.Constant;
import me.iroohom.util.DateUtil;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @ClassName: StockIncrFunction
 * @Author: Roohom
 * @Function: 个股分时窗口数据处理
 * @Date: 2020/11/2 16:19
 * @Software: IntelliJ IDEA
 */
public class StockIncrWindowFunction extends RichWindowFunction<CleanBean, StockIncrBean, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<CleanBean> input, Collector<StockIncrBean> out) throws Exception {
        CleanBean cleanBean = null;
        for (CleanBean bean : input) {
            if (cleanBean == null) {
                cleanBean = bean;
            }
            if (cleanBean.getEventTime() < bean.getEventTime()) {
                cleanBean = bean;
            }
        }

        Long tradeTime = DateUtil.longTimeTransfer(cleanBean.getEventTime(), Constant.format_YYYYMMDDHHMMSS);


        /**指标计算
         * 涨跌、涨跌幅、振幅
         * 今日涨跌=当前价-前收盘价
         * 今日涨跌幅（%）=（当前价-前收盘价）/ 前收盘价 * 100%
         * 今日振幅 =（当日最高点的价格－当日最低点的价格）/昨天收盘价×100% ，反应价格波动情况
         */
        //涨跌
        BigDecimal updown = cleanBean.getTradePrice().subtract(cleanBean.getPreClosePrice());
        //涨跌幅
        BigDecimal increase = updown.divide(cleanBean.getPreClosePrice(), 2, RoundingMode.HALF_UP);
        //振幅
        BigDecimal amplitude = (cleanBean.getMaxPrice().subtract(cleanBean.getMinPrice())).divide(cleanBean.getPreClosePrice(), 2, RoundingMode.HALF_UP);

        /**
         * 封装数据输出
         * eventTime、secCode、secName、increase、tradePrice、updown、tradeVol、amplitude、
         * preClosePrice、tradeAmt、tradeTime、source
         */

        out.collect(new StockIncrBean(
                cleanBean.getEventTime(),
                cleanBean.getSecCode(),
                cleanBean.getSecName(),
                increase,
                cleanBean.getTradePrice(),
                updown,
                cleanBean.getTradeVolume(),
                amplitude,
                cleanBean.getPreClosePrice(),
                cleanBean.getTradeAmt(),
                tradeTime,
                cleanBean.getSource()
        ));

    }
}
