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
 * @ClassName: IndexSecTask
 * @Author: Roohom
 * @Function: 指数秒级行情
 * @Date: 2020/11/5 13:01
 * @Software: IntelliJ IDEA
 */
public class IndexSecTask implements ProcessDataInterface {

    /**
     * 开发步骤：
     * 1.数据分组
     * 2.划分时间窗口
     * 3.秒级数据处理（新建数据写入Bean和秒级窗口函数）
     * 4.数据写入操作
     *   * 封装ListPuts
     *   * 数据写入
     */
    @Override
    public void process(DataStream<CleanBean> waterData) {
        //数据分组
        waterData.keyBy(new KeyFunction())
                //划分时间窗口
                .timeWindow(Time.seconds(5))
                //秒级窗口数据处理（新建数据写入Bean和秒级窗口函数）
                .apply(new SecIndexWindowFunction())
                //封装ListPuts
                .timeWindowAll(Time.seconds(5))
                //封装数据成List<Puts>写入Hbase的处理函数
                .apply(new SecIndexHbaseFunction())
                //数据写入Hbase
                .addSink(new SinkHbase(QuotConfig.config.getProperty("index.hbase.table.name")));
    }
}
