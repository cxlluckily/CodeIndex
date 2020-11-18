package me.iroohom.task;

import me.iroohom.bean.CleanBean;
import me.iroohom.bean.StockBean;
import me.iroohom.config.QuotConfig;
import me.iroohom.function.KeyFunction;
import me.iroohom.function.MinStockWindowFunction;
import me.iroohom.inter.ProcessDataInterface;
import me.iroohom.map.StockPutHdfsMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

/**
 * @ClassName: StockMinHdfsTask
 * @Author: Roohom
 * @Function: 个股分时行情写入Hdfs备份
 * @Date: 2020/11/2 15:39
 * @Software: IntelliJ IDEA
 */
public class StockMinHdfsTask implements ProcessDataInterface {
    /**
     * 开发步骤：
     * 1.设置HDFS存储路径
     * 2.设置数据文件参数
     * （大小、分区、格式、前缀、后缀）
     * 3.数据分组
     * 4.划分时间窗口
     * 5.数据处理
     * 6.转换并封装数据
     * 7.写入HDFS
     *
     * @param waterData 水位线数据
     */

    @Override
    public void process(DataStream<CleanBean> waterData) {
        //基于Flink-1.7.0实时写数据到文件
        //构建写文件对象
        BucketingSink<String> bucketingSink = new BucketingSink<>(QuotConfig.config.getProperty("stock.sec.hdfs.path"));
        //设置数据文件参数
        //批大小
        bucketingSink.setBatchSize(Long.valueOf(QuotConfig.config.getProperty("hdfs.batch")));
        //按时间分区
        bucketingSink.setBucketer(new DateTimeBucketer<>(QuotConfig.config.getProperty("hdfs.bucketer")));
        //格式 -> String
        bucketingSink.setWriter(new StringWriter<>());
        //前缀、后缀
        bucketingSink.setInProgressPrefix("stock-");
        bucketingSink.setPendingPrefix("stock-2");
        bucketingSink.setInProgressSuffix(".txt");
        bucketingSink.setPendingSuffix(".txt");

        //数据分组
        waterData.keyBy(new KeyFunction())
                .timeWindow(Time.minutes(1))
                .apply(new MinStockWindowFunction())
                .map(new StockPutHdfsMap())
                .addSink(bucketingSink);
    }
}













