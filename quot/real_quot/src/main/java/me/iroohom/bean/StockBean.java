package me.iroohom.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @Date 2020/11/1
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StockBean {


    /**
     * eventTime、secCode、secName、preClosePrice、openPrice、highPrice、lowPrice、closePrice、
     * tradeVol、tradeAmt、tradeVolDay、tradeAmtDay、tradeTime、source
     */
    private Long eventTime;
    private String secCode;
    private String secName;
    private BigDecimal preClosePrice;
    private BigDecimal openPrice;
    private BigDecimal highPrice;
    private BigDecimal lowPrice;
    private BigDecimal closePrice;
    //分时成交量 （当前分钟的总成交量- 上一分钟的总成交量）
    private Long tradeVol;
    //分时成交金额 （当前分钟的总成交金额- 上一分钟的总成交金额）
    private Long tradeAmt;
    //总成交量
    private Long tradeVolDay;
    //总成交金额
    private Long tradeAmtDay;
    //格式化之后的事件时间,做rowkey拼接使用
    private Long tradeTime;
    private String source;
}
