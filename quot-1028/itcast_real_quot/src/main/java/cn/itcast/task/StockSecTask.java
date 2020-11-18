package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeyFunction;
import cn.itcast.function.SecStockHbaseFunction;
import cn.itcast.function.SecStockWindowFunction;
import cn.itcast.inter.ProcessDataInterface;
import cn.itcast.sink.SinkHbase;
import cn.itcast.util.HbaseUtil;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Date 2020/11/1
 * @Describetion 秒级行情
 */
public class StockSecTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {

        /**
         * 1.每5s生成一条最新行情数据(事件时间最大的数据，就是最新一条行情数据)
         * 2.批量插入 ，需要把数据封装大豆List<Put>
         * 3.窗口时间5s
         */
        /**
         * 开发步骤：
         * 1.数据分组
         * 2.划分时间窗口
         * 3.新建个股数据写入bean对象
         * 4.秒级窗口函数业务处理
         * 5.数据写入操作
         *   * 封装ListPuts
         *   * 数据写入
         */
        //1.数据分组
        waterData.keyBy(new KeyFunction()) //根据个股代码进行分组
                //2.划分时间窗口
                .timeWindow(Time.seconds(5))
                //4.秒级窗口函数业务处理
                .apply(new SecStockWindowFunction())
                .timeWindowAll(Time.seconds(5)) //不分组划分时间窗口
                //5.数据写入操作
                //封装ListPuts
                .apply(new SecStockHbaseFunction()) //收集的是这5s窗口之内的所有数据,获取结果数据：List<Put>
                //数据写入
                .addSink(new SinkHbase(QuotConfig.config.getProperty("stock.hbase.table.name")));
    }
}
