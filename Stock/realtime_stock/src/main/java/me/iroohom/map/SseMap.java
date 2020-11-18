package me.iroohom.map;

import me.iroohom.avro.SseAvro;
import me.iroohom.bean.CleanBean;
import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;

/**
 * @ClassName: SseMap
 * @Author: Roohom
 * @Function: 沪市数据处理函数 用来作数据合并之前的处理
 * @Date: 2020/11/15 21:11
 * @Software: IntelliJ IDEA
 */
public class SseMap implements MapFunction<SseAvro, CleanBean> {

    @Override
    public CleanBean map(SseAvro value) throws Exception {
        CleanBean cleanBean = new CleanBean();
        cleanBean.setMdStreamId(value.getMdStreamID().toString());
        cleanBean.setSecCode(value.getSecurityID().toString());
        cleanBean.setSecName(value.getSymbol().toString());
        cleanBean.setTradeVolume(value.getTradeVolume());
        cleanBean.setTradeAmt(value.getTotalValueTraded());
        cleanBean.setPreClosePrice(BigDecimal.valueOf(value.getPreClosePx()));
        cleanBean.setOpenPrice(BigDecimal.valueOf(value.getOpenPrice()));
        cleanBean.setMaxPrice(BigDecimal.valueOf(value.getHighPrice()));
        cleanBean.setMinPrice(BigDecimal.valueOf(value.getLowPrice()));
        cleanBean.setTradePrice(BigDecimal.valueOf(value.getTradePrice()));
        /**
         * 时间时间
         */
        cleanBean.setEventTime(value.getTimestamp());
        /**
         * 数据来源
         */
        cleanBean.setSource("sse");

        return cleanBean;
    }
}
