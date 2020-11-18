package me.iroohom.task;

import me.iroohom.bean.CleanBean;
import me.iroohom.config.QuotConfig;
import me.iroohom.function.KeyFunction;
import me.iroohom.function.SecStockHbaseFunction;
import me.iroohom.function.SecStockWindowFunction;
import me.iroohom.inter.ProcessDataInterface;
import me.iroohom.sink.SinkHbase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName: StockSecTask
 * @Author: Roohom
 * @Function: 个股秒级业务处理函数
 * @Date: 2020/11/1 09:43
 * @Software: IntelliJ IDEA
 */
public class StockSecTask implements ProcessDataInterface {
    /**
     * 每五秒生成一条最新数据，批量插入（时间最大，数据最新）
     * 窗口时间5秒
     * 开发步骤：
     * 1.数据分组
     * 2.划分时间窗口
     * 3.新建个股数据写入bean对象
     * 4.秒级窗口函数业务处理
     * 5.数据写入操作
     * * 封装ListPuts
     * * 数据写入
     *
     * @param waterData 水位线数据
     */
    @Override
    public void process(DataStream<CleanBean> waterData) {
        waterData
                //数据分组
                .keyBy(new KeyFunction())
                //划分时间窗口
                .timeWindow(Time.seconds(5))
                //秒级窗口函数业务处理
                .apply(new SecStockWindowFunction())
                //不分组划分时间窗口
                .timeWindowAll(Time.seconds(5))
                //封装PutList，写入Hbase
                .apply(new SecStockHbaseFunction())
                //写入Hbase
                .addSink(new SinkHbase(QuotConfig.config.getProperty("stock.hbase.table.name")));
    }
}
