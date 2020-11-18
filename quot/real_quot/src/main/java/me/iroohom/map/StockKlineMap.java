package me.iroohom.map;

import me.iroohom.bean.StockBean;
import me.iroohom.constant.Constant;
import me.iroohom.util.DateUtil;
import me.iroohom.util.DbUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Map;

/**
 * @ClassName: StockKlineMap
 * @Author: Roohom
 * @Function: 个股K线数据转换Map
 * @Date: 2020/11/4 19:38
 * @Software: IntelliJ IDEA
 */
public class StockKlineMap extends RichMapFunction<StockBean, Row> {

    /**
     * K线类型
     */
    private String kType;
    /**
     * 周期首个交易日字段名
     */
    private String firstTxdate;

    //构造方法
    public StockKlineMap(String kType, String firstTxdate) {
        this.kType = kType;
        this.firstTxdate = firstTxdate;
    }


    String tradeDate = null;
    String firstTradeDate = null;
    Map<String, Map<String, Object>> klineMap;

    @Override
    public void open(Configuration parameters) throws Exception {
        //获取交易日历表最新交易日数据
        String sql = "SELECT * FROM  tcc_date WHERE trade_date <=  CURDATE() ORDER BY trade_date DESC LIMIT 1";
        Map<String, String> dateMap = DbUtil.queryKv(sql);
        /**
         * 获取首个交易日和T日
         *
         * 如果是日K tradeDate = firstTradeDate
         * 如果是周K/月K tradeDate >= firstTradeDate
         * (如果T日是周期首个交易日，日/周/月，都是同一天，如果T日不是是首个交易日，tradeDate > firstTradeDate)
         */
        //获取T日
        tradeDate = dateMap.get("trade_date");
        //获取首个交易日
        firstTradeDate = dateMap.get(firstTxdate);

        //获取K（周，月）线下的汇总表数据（高、低、成交量、金额）
        String sqlKline = "SELECT sec_code ,MAX(high_price) AS high_price,MIN(low_price) AS low_price ,SUM(trade_amt) AS trade_amt,\n" +
                "SUM(trade_vol) AS trade_vol FROM bdp_quot_stock_kline_day \n" +
                "WHERE trade_date BETWEEN " + firstTradeDate + " AND " + tradeDate + " \n" +
                "GROUP BY 1";
        klineMap = DbUtil.query("sec_code", sqlKline);
    }

    /**
     * 二、业务处理
     * 1.获取个股部分数据（前收、收、开盘、高、低、量、金额）
     * 2.获取T日和周首次交易日时间,转换成long型
     * 3.比较周期首个交易日和当天交易日大小，判断是否是周、月K线
     * 4.获取周/月K数据：成交量、成交额、高、低
     * 5.高、低价格比较
     * 6.计算成交量、成交额
     * 7.计算均价
     * 8.封装数据Row
     */
    @Override
    public Row map(StockBean value) throws Exception {
        //获取个股部分数据（前收、收、开盘、高、低、量、金额）
        BigDecimal preClosePrice = value.getPreClosePrice();
        BigDecimal closePrice = value.getClosePrice();
        BigDecimal openPrice = value.getOpenPrice();
        BigDecimal highPrice = value.getHighPrice();
        BigDecimal lowPrice = value.getLowPrice();
        Long tradeVolDay = value.getTradeVolDay();
        Long tradeAmtDay = value.getTradeAmtDay();

        //获取T日和周首次交易日时间，转换成Long类型
        Long tradeTime = DateUtil.stringToLong(tradeDate, Constant.format_yyyy_mm_dd);
        Long firstTradeTime = DateUtil.stringToLong(firstTradeDate, Constant.format_yyyy_mm_dd);

        //比较周期收个交易日和当天交易日大小，判断是否是周K、月K线
        if (firstTradeTime < tradeTime && ("2".equals(kType) || "3".equals(kType))) {
            //获取周/月K线的数据：成交量，成交金额，高价、低价
            Map<String, Object> map = klineMap.get(value.getSecCode());
            if (map != null && map.size() > 0) {
                //历史成交总量
                Long tradeVol = Long.valueOf(map.get("trade_vol").toString());
                //历史成交总金额
                Long tradeAmt = Long.valueOf(map.get("trade_amt").toString());
                //周期内的最新总成交量
                tradeVolDay += tradeVol;
                //周期内的最新总成交金额
                tradeAmtDay += tradeAmt;

                BigDecimal high_price = new BigDecimal(map.get("high_price").toString());
                BigDecimal low_price = new BigDecimal(map.get("low_price").toString());

                //最高价和最低价的比较与替换
                if (highPrice.compareTo(high_price) < 0) {
                    highPrice = high_price;
                }
                if (lowPrice.compareTo(low_price) > 0) {
                    lowPrice = low_price;
                }
            }
        }

        //计算均价,成交金额/成交总量
        BigDecimal avgPrice = new BigDecimal(0);
        if (tradeVolDay != 0) {
            avgPrice = new BigDecimal(tradeAmtDay).divide(new BigDecimal(tradeVolDay), 2, BigDecimal.ROUND_HALF_UP);
        }

        //封装数据Row
        Row row = new Row(13);

        row.setField(0, new Timestamp(System.currentTimeMillis()));
        row.setField(1, tradeDate);
        row.setField(2, value.getSecCode());
        row.setField(3, value.getSecName());
        row.setField(4, kType);
        row.setField(5, preClosePrice);
        row.setField(6, openPrice);
        row.setField(7, highPrice);
        row.setField(8, lowPrice);
        row.setField(9, closePrice);
        row.setField(10, avgPrice);
        row.setField(11, tradeVolDay);
        row.setField(12, tradeAmtDay);
        return row;
    }
}
