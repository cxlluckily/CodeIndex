package me.iroohom.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @Date 2020/10/31
 * 数据清洗bean对象，接收avro对象数据
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CleanBean {

    /**
     * 行情数据类型
     */
    private String mdStreamId;
    /**
     * 证券代码
     */
    private String secCode;
    /**
     * 证券名称
     */
    private String secName;
    /**
     * 成交数量
     */
    private Long tradeVolume;
    /**
     * 成交金额
     */
    private Long tradeAmt;
    /**
     * 昨日收盘价
     */
    private BigDecimal preClosePrice;
    /**
     * 开盘价
     */
    private BigDecimal openPrice;
    /**
     * 最高价
     */
    private BigDecimal maxPrice;
    /**
     * 最低价
     */
    private BigDecimal minPrice;
    /**
     * 成交价（最新价）
     */
    private BigDecimal tradePrice;
    /**
     * 时间戳
     */
    private Long eventTime;
    /**
     * 数据来源
     */
    private String source;
}
