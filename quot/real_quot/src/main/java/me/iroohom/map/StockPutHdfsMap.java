package me.iroohom.map;

import me.iroohom.bean.StockBean;
import me.iroohom.config.QuotConfig;
import me.iroohom.constant.Constant;
import me.iroohom.util.DateUtil;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.sql.Timestamp;

/**
 * @ClassName: StockPutHdfsMap
 * @Author: Roohom
 * @Function:
 * @Date: 2020/11/2 15:54
 * @Software: IntelliJ IDEA
 */
public class StockPutHdfsMap extends RichMapFunction<StockBean, String> {
    /**
     * 定义字符串分隔符
     */
    String sep = QuotConfig.config.getProperty("hdfs.seperator");

    /**
     * 开发步骤:
     * 1.定义字符串字段分隔符
     * 2.日期转换和截取：date类型
     * 3.新建字符串缓存对象
     * 4.封装字符串数据
     * <p>
     * 字符串拼装字段顺序：
     * Timestamp|date|secCode|secName|preClosePrice|openPrice|highPrice|
     * lowPrice|closePrice|tradeVol|tradeAmt|tradeVolDay|tradeAmtDay|source
     */
    @Override
    public String map(StockBean value) throws Exception {
        String tradeDate = DateUtil.longTimeToString(value.getEventTime(), Constant.format_yyyy_mm_dd);

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(new Timestamp(value.getEventTime())).append(sep)
                .append(tradeDate).append(sep)
                .append(value.getSecName()).append(sep)
                .append(value.getPreClosePrice()).append(sep)
                .append(value.getOpenPrice()).append(sep)
                .append(value.getHighPrice()).append(sep)
                .append(value.getLowPrice()).append(sep)
                .append(value.getClosePrice()).append(sep)
                .append(value.getTradeVol()).append(sep)
                .append(value.getTradeAmt()).append(sep)
                .append(value.getTradeVolDay()).append(sep)
                .append(value.getTradeAmtDay()).append(sep)
                .append(value.getSource());

        return stringBuilder.toString();
    }
}
