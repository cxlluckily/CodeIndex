package me.iroohom.function;


import me.iroohom.bean.CleanBean;
import me.iroohom.bean.StockBean;
import me.iroohom.constant.Constant;
import me.iroohom.util.DateUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName: MinStockWindowFunction
 * @Author: Roohom
 * @Function: 分时个股窗口处理
 * @Date: 2020/11/2 15:00
 * @Software: IntelliJ IDEA
 */
public class MinStockWindowFunction extends RichWindowFunction<CleanBean, StockBean, String, TimeWindow> {

    MapState<String, StockBean> stockMs = null;

    /**
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        stockMs = getRuntimeContext().getMapState(new MapStateDescriptor<String, StockBean>("stockMs", String.class, StockBean.class));
    }

    /**
     * @param s      分组字段
     * @param window 窗口
     * @param input  输入数据
     * @param out    输出数据
     * @throws Exception PASS
     */
    @Override
    public void apply(String s, TimeWindow window, Iterable<CleanBean> input, Collector<StockBean> out) throws Exception {
        //记录最新个股
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
        StockBean stockBeanLast = stockMs.get(cleanBean.getSecCode());
        Long tradeVol = 0L;
        Long tradeAmt = 0L;
        if (stockBeanLast != null) {
            //获取上一分钟的成交金额和成交数量
            Long tradeVolDayLast = stockBeanLast.getTradeVolDay();
            Long tradeAmtDayLast = stockBeanLast.getTradeAmtDay();

            //分时成交量,分时成交量 （当前分钟的总成交量- 上一分钟的总成交量）
            tradeVol = cleanBean.getTradeVolume() - tradeVolDayLast;
            //分时成交金额,分时成交金额 （当前分钟的总成交金额- 上一分钟的总成交金额）
            tradeAmt = cleanBean.getTradeAmt() - tradeAmtDayLast;
        }
        Long tradeTime = DateUtil.longTimeTransfer(cleanBean.getEventTime(), Constant.format_YYYYMMDDHHMMSS);

        //封装输出流数据
        StockBean stockBean = new StockBean(
                cleanBean.getEventTime(),
                cleanBean.getSecCode(),
                cleanBean.getSecName(),
                cleanBean.getPreClosePrice(),
                cleanBean.getOpenPrice(),
                cleanBean.getMaxPrice(),
                cleanBean.getMinPrice(),
                cleanBean.getTradePrice(),
                tradeVol, tradeAmt,
                cleanBean.getTradeVolume(),
                cleanBean.getTradeAmt(),
                tradeTime,
                cleanBean.getSource()
        );
        out.collect(stockBean);
        //更新MapState
        stockMs.put(cleanBean.getSecCode(), stockBean);
    }
}
