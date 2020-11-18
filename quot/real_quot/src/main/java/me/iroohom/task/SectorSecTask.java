package me.iroohom.task;

import me.iroohom.bean.CleanBean;
import me.iroohom.config.QuotConfig;
import me.iroohom.function.KeyFunction;
import me.iroohom.function.MinStockWindowFunction;
import me.iroohom.function.SectorHbaseListWindowFunction;
import me.iroohom.function.SectorWindowFunction;
import me.iroohom.inter.ProcessDataInterface;
import me.iroohom.sink.SinkHbase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName: SectorSecTask
 * @Author: Roohom
 * @Function:
 * @Date: 2020/11/3 19:33
 * @Software: IntelliJ IDEA
 */
public class SectorSecTask implements ProcessDataInterface {
    /**
     * 开发步骤：
     * 1.数据分组
     * 2.划分时间窗口
     * 3.个股数据处理
     * 4.划分时间窗口
     * 5.秒级数据处理（新建数据写入样例类和秒级窗口函数）
     * 6.数据写入操作
     * * 封装ListPuts
     * * 数据写入
     */
    @Override
    public void process(DataStream<CleanBean> waterData) {

        waterData
                //分组
                .keyBy(new KeyFunction())
                //划分时间窗口
                .timeWindow(Time.seconds(5))
                //个股数据处理，板块基于个股数据开发，必须提前获取个股数据，后续用秒级窗口和分时窗口业务合并发开，所以暂时用分时个股窗口数据处理
                .apply(new MinStockWindowFunction())
                //划分时间窗口
                .timeWindowAll(Time.seconds(5))
                //秒级数据处理，新建数据写入样例类和秒级窗口函数
                //此窗口获取板块数据
                .apply(new SectorWindowFunction())
                .timeWindowAll(Time.seconds(5))
                .apply(new SectorHbaseListWindowFunction())
                .addSink(new SinkHbase(QuotConfig.config.getProperty("sector.hbase.table.name")));
    }
}
