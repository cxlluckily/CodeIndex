package me.iroohom.cron;

import me.iroohom.mapper.QuotMapper;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * @ClassName: CronTask
 * @Author: Roohom
 * @Function: 定时任务
 * @Date: 2020/11/9 21:55
 * @Software: IntelliJ IDEA
 */
//开启定时任务
@EnableScheduling
@Component
public class CronTask {

    @Autowired
    QuotMapper quotMapper;

    // 0/10 * * * * ? 表示没10s执行一次
    @Scheduled(cron = "${cron.pattern.loader}")
    public void cronTask()
    {
        Map<String, Object> map = quotMapper.queryTccDate();
        String tradeDate = map.get("trade_date").toString();
        String weekFirstTxdate = map.get("week_first_txdate").toString();
        String monthFirstTxdate = map.get("month_first_txdate").toString();
        List<Map<String, Object>> weekList = quotMapper.queryKline("bdp_quot_stock_kline_week", weekFirstTxdate, tradeDate);
        if (weekList!=null && weekList.size()>0)
        {
            quotMapper.updateKline("bdp_quot_stock_kline_week",weekFirstTxdate,tradeDate);
        }
        List<Map<String, Object>> monthList = quotMapper.queryKline("bdp_quot_stock_kline_week", weekFirstTxdate, tradeDate);
        if (monthList!=null&&monthList.size()>0)
        {
            quotMapper.updateKline("bdp_quot_stock_kline_month", monthFirstTxdate, tradeDate);
        }
    }
}
