package me.iroohom.map;

import me.iroohom.bean.IndexBean;
import me.iroohom.config.QuotConfig;
import me.iroohom.constant.Constant;
import me.iroohom.util.DateUtil;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.sql.Timestamp;

/**
 * @ClassName: IndexPutHdfsMap
 * @Author: Roohom
 * @Function: 指数分时行情备份至HDFS转换Map
 * @Date: 2020/11/5 17:23
 * @Software: IntelliJ IDEA
 */
public class IndexPutHdfsMap extends RichMapFunction<IndexBean, String> {


    /**
     * 开发步骤:
     * 1.定义字符串字段分隔符
     * 2.日期转换和截取：date类型
     * 3.新建字符串缓存对象
     * 4.封装字符串数据
     * <p>
     * 字符串拼装字段顺序：
     * Timestamp|date|indexCode|indexName|preClosePrice|openPirce|highPrice|
     * lowPrice|closePrice|tradeVol|tradeAmt|tradeVolDay|tradeAmtDay|source
     */

    //指定分割符
    String sep  = QuotConfig.config.getProperty("hdfs.seperator");
    @Override
    public String map(IndexBean value) throws Exception {
        //日期转换截取
        String tradeDate = DateUtil.longTimeToString(value.getEventTime(), Constant.format_yyyy_mm_dd);
        StringBuilder builder = new StringBuilder();
        builder.append(new Timestamp(value.getEventTime())).append(sep)
                .append(tradeDate).append(sep)
                .append(value.getIndexCode()).append(sep)
                .append(value.getIndexName()).append(sep)
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


        return builder.toString();
    }
}
