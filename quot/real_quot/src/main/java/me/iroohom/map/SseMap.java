package me.iroohom.map;

import me.iroohom.avro.SseAvro;
import me.iroohom.bean.CleanBean;
import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;

/**
 * @ClassName: SseMap
 * @Author: Roohom
 * @Function: Map用于将SseAvro解析成CleanBean
 * @Date: 2020/11/1 19:37
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
        //事件时间
        cleanBean.setEventTime(value.getTimestamp());
        //表示数据来源于沪市
        cleanBean.setSource("sse");
        return cleanBean;
    }
}
