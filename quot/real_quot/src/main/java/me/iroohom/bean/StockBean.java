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
    //事件时间
    private Long eventTime;
    //证券代码
    private String secCode;
    //证券名称
    private String secName;
    //前收盘价
    private BigDecimal preClosePrice;
    //开盘价
    private BigDecimal openPrice;
    //最高价
    private BigDecimal highPrice;
    //最低价
    private BigDecimal lowPrice;
    //收盘价
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
