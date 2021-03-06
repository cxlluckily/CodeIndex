package me.iroohom.task;

import me.iroohom.bean.CleanBean;
import me.iroohom.config.QuotConfig;
import me.iroohom.function.KeyFunction;
import me.iroohom.function.SecIndexHbaseFunction;
import me.iroohom.function.SecIndexWindowFunction;
import me.iroohom.inter.ProcessDataInterface;
import me.iroohom.sink.SinkHbase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Date 2020/11/1
 * 指数秒级行情业务
 */
public class IndexSecTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {

        /**
         * 开发步骤：
         * 1.数据分组
         * 2.划分时间窗口
         * 3.秒级数据处理（新建数据写入Bean和秒级窗口函数）
         * 4.数据写入操作
         *   * 封装ListPuts
         *   * 数据写入
         */
        //1.数据分组
        waterData.keyBy(new KeyFunction())
                //2.划分时间窗口
                .timeWindow(Time.seconds(5))
                //3.秒级数据处理（新建数据写入Bean和秒级窗口函数）
                .apply(new SecIndexWindowFunction())
                .timeWindowAll(Time.seconds(5))
                // 4.数据写入操作
                //封装ListPuts
                .apply(new SecIndexHbaseFunction())
                //数据写入
                .addSink(new SinkHbase(QuotConfig.config.getProperty("index.hbase.table.name")));

    }
}
