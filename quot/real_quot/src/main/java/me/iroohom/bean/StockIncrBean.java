package me.iroohom.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @ClassName: StockIncrBean
 * @Author: Roohom
 * @Function:
 * @Date: 2020/11/2 16:15
 * @Software: IntelliJ IDEA
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class StockIncrBean {


    /**
     * 字段如下：
     * eventTime、secCode、secName、increase、tradePrice、updown、tradeVol、amplitude、
     * preClosePrice、tradeAmt、tradeTime、source
     */
    private Long eventTime;
    private String secCode;
    private String secName;
    //涨跌幅
    private BigDecimal increase;
    //最新价
    private BigDecimal tradePrice;
    //涨跌
    private BigDecimal updown;
    //总手/总成交量
    private Long tradeVol;
    //振幅
    private BigDecimal amplitude;
    private BigDecimal preClosePrice;
    //总成交金额
    private Long tradeAmt;
    //格式化时间
    private Long tradeTime;
    private String source;
}
