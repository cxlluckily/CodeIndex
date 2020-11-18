package me.iroohom.map;

import me.iroohom.bean.SectorBean;
import me.iroohom.constant.Constant;
import me.iroohom.util.DateUtil;
import me.iroohom.util.DbUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.Map;

/**
 * @ClassName: SectorKlineMap
 * @Author: Roohom
 * @Function: 板块K线 Map函数
 * @Date: 2020/11/5 10:10
 * @Software: IntelliJ IDEA
 */
public class SectorKlineMap extends RichMapFunction<SectorBean, Row> {
    /**
     * 一、初始化
     * 1.创建构造方法
     * 入参：kType：K线类型
     * firstTxdate：周期首个交易日
     * 2.获取交易日历表交易日数据
     * 3.获取周期首个交易日和T日
     * 4.获取K线下的汇总表数据（高、低、成交量、金额）
     */
    private String kType;
    private String firstTxDate;

    //构造方法
    public SectorKlineMap(String kType, String firstTxDate) {
        this.kType = kType;
        this.firstTxDate = firstTxDate;
    }

    Long tradeTime = 0L;
    Long firstTradeTime = 0L;
    String tradeDate;
    Map<String, Map<String, Object>> klineMap;

    /**
     * 初始化方法
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        //获取交易日历数据
        String sql = "SELECT * FROM tcc_date WHERE trade_date <= CURDATE() ORDER BY trade_date DESC LIMIT 1";
        Map<String, String> dateMap = DbUtil.queryKv(sql);

        //获取周期首个交易日和T日
        //T日
        tradeDate = dateMap.get("trade_date");
        //周期首个交易日
        String firstTradeDate = dateMap.get(firstTxDate);

        //获取T日和首次交易时间，转换成Long型
        tradeTime = DateUtil.stringToLong(tradeDate, Constant.format_yyyy_mm_dd);
        firstTradeTime = DateUtil.stringToLong(firstTradeDate, Constant.format_yyyy_mm_dd);

        //获取K线下的汇总表数据（高、低、成交量、成交金额）
        String sqlKline = "SELECT sector_code ,MAX(high_price) as high_price ,MIN(low_price) as low_price," +
                "SUM(trade_vol) as trade_vol,SUM(trade_amt) as trade_amt \n" +
                "FROM bdp_quot_sector_kline_day\n" +
                "WHERE trade_date BETWEEN " + firstTradeDate + "  AND " + tradeDate + " \n" +
                "GROUP BY 1";
        klineMap = DbUtil.query("sector_code", sqlKline);

    }

    /**
     * 开发步骤：
     * 1.获取个股部分数据（前收、收、开盘、高、低、量、金额）
     * 2.获取T日和周首次交易日时间,转换成long型
     * 3.比较周期首个交易日和当天交易日大小，判断是否是周、月K线
     * 4.获取周/月K数据：成交量、成交额、高、低
     * 5.高、低价格比较
     * 6.计算均价
     * 7.封装数据Row
     */
    @Override
    public Row map(SectorBean value) throws Exception {
        BigDecimal preClosePrice = value.getPreClosePrice();
        BigDecimal closePrice = value.getClosePrice();
        BigDecimal openPrice = value.getOpenPrice();
        BigDecimal highPrice = value.getHighPrice();
        BigDecimal lowPrice = value.getLowPrice();
        Long tradeVolDay = value.getTradeVolDay();
        Long tradeAmtDay = value.getTradeAmtDay();

        //比较周期首个交易日和当天交易日大小，判定是否是周、月K线
        if (firstTradeTime < tradeTime && ("2".equals(kType) || "3".equals(kType))) {
            //首个交易日的时间小于交易时间，肯定不是日K，而是周K或者月K
            Map<String, Object> map = klineMap.get(value.getSectorCode());
            if (map != null) {
                //周期内的历史数据
                BigDecimal high_price = new BigDecimal(map.get("high_price").toString());
                BigDecimal low_price = new BigDecimal(map.get("low_price").toString());
                Long trade_vol = Long.valueOf(map.get("trade_vol").toString());
                Double trade_amt = Double.valueOf(map.get("trade_amt").toString());

                //获取最新总量数据（成交金额/量）
                tradeVolDay += trade_vol;
                tradeAmtDay += trade_amt.longValue();

                if (highPrice.compareTo(high_price) < 0) {
                    highPrice = high_price;
                }
                if (lowPrice.compareTo(low_price) > 0) {
                    lowPrice = low_price;
                }
            }
        }

        //日K数据直接进入以下处理逻辑
        //计算均价 历史总交易金额/总交易量
        BigDecimal avgPrice = new BigDecimal(0);
        if (tradeVolDay != 0) {
            //总金额除以总交易量
            avgPrice = new BigDecimal(tradeAmtDay).divide(new BigDecimal(tradeVolDay), 2, RoundingMode.HALF_UP);
        }

        //封装数据
        Row row = new Row(13);

        row.setField(0, new Timestamp(System.currentTimeMillis()));
        row.setField(1, tradeDate);
        row.setField(2, value.getSectorCode());
        row.setField(3, value.getSectorName());
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
