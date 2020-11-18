package me.iroohom.map;

import me.iroohom.avro.SzseAvro;
import me.iroohom.bean.CleanBean;
import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;

/**
 * @ClassName: SzseMap
 * @Author: Roohom
 * @Function: 深市数据处理Map函数 用作在和深市数据合并之前的数据清洗转化
 * @Date: 2020/11/15 21:25
 * @Software: IntelliJ IDEA
 */
public class SzseMap implements MapFunction<SzseAvro, CleanBean> {

    @Override
    public CleanBean map(SzseAvro value) throws Exception {
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
        cleanBean.setSource("szse");

        return null;
    }
}
