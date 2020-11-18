package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeyFunction;
import cn.itcast.function.MinStockWindowFunction;
import cn.itcast.function.SectorHbaseListWindowFunction;
import cn.itcast.function.SectorWindowFunction;
import cn.itcast.inter.ProcessDataInterface;
import cn.itcast.sink.SinkHbase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Date 2020/11/3
 * 板块秒级业务
 */
public class SectorSecTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {

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
        //1.数据分组
        waterData.keyBy(new KeyFunction())
                .timeWindow(Time.seconds(5))
                //3.个股数据处理,板块是基于个股数据开发，必须提前获取个股数据
                .apply(new MinStockWindowFunction()) //后续开发会将秒级窗口和分时窗口业务合并开发，所以用分时个股窗口数据处理
                //4.划分时间窗口
                .timeWindowAll(Time.seconds(5)) //不分组，获取所有个股数据
                //5.秒级数据处理（新建数据写入样例类和秒级窗口函数）
                //在此窗口获取板块数据
                .apply(new SectorWindowFunction())
                //6.数据写入操作
                .timeWindowAll(Time.seconds(5))
                .apply(new SectorHbaseListWindowFunction())
                //数据写入
                .addSink(new SinkHbase(QuotConfig.config.getProperty("sector.hbase.table.name")));

    }
}
