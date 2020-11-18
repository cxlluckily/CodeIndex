package me.iroohom.map;

import me.iroohom.bean.SectorBean;
import me.iroohom.config.QuotConfig;
import me.iroohom.constant.Constant;
import me.iroohom.util.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;

import java.sql.Timestamp;

/**
 * @ClassName: SecterPutHdfsMap
 * @Author: Roohom
 * @Function: 板块数据put到HDFS的Map
 * @Date: 2020/11/4 18:30
 * @Software: IntelliJ IDEA
 */
public class SecterPutHdfsMap implements MapFunction<SectorBean, String> {
    /**
     * 开发步骤:
     * 1.定义字符串字段分隔符
     * 2.日期转换和截取：date类型
     * 3.新建字符串缓存对象
     * 4.封装字符串数据
     * 字符串拼装字段顺序：
     * Timestamp|date|sectorCode|sectorName|preClosePrice|openPirce|highPrice|
     * lowPrice|closePrice|tradeVol|tradeAmt|tradeVolDay|tradeAmtDay
     */

    String sep = QuotConfig.config.getProperty("hdfs.seperator");

    @Override
    public String map(SectorBean value) throws Exception {
        String tradeDate = DateUtil.longTimeToString(value.getEventTime(), Constant.format_yyyy_mm_dd);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(new Timestamp(value.getEventTime())).append(sep)
                .append(tradeDate).append(sep)
                .append(value.getSectorCode()).append(sep)
                .append(value.getSectorName()).append(sep)
                .append(value.getPreClosePrice()).append(sep)
                .append(value.getOpenPrice()).append(sep)
                .append(value.getHighPrice()).append(sep)
                .append(value.getLowPrice()).append(sep)
                .append(value.getClosePrice()).append(sep)
                .append(value.getTradeVol()).append(sep)
                .append(value.getTradeAmt()).append(sep)
                .append(value.getTradeVolDay()).append(sep)
                .append(value.getTradeAmtDay());
        return stringBuilder.toString();
    }
}
