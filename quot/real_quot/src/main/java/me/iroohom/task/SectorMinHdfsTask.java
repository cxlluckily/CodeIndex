package me.iroohom.task;

import me.iroohom.bean.CleanBean;
import me.iroohom.config.QuotConfig;
import me.iroohom.function.KeyFunction;
import me.iroohom.function.MinStockWindowFunction;
import me.iroohom.function.SectorWindowFunction;
import me.iroohom.inter.ProcessDataInterface;
import me.iroohom.map.SecterPutHdfsMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

/**
 * @ClassName: SectorMinHdfsTask
 * @Author: Roohom
 * @Function: 板块数据分时写入HDFS备份
 * @Date: 2020/11/4 18:19
 * @Software: IntelliJ IDEA
 */
public class SectorMinHdfsTask implements ProcessDataInterface {
    /**
     * 开发步骤：
     * 1.设置HDFS存储路径
     * 2.设置数据文件参数
     *  （大小、分区、格式、前缀、后缀）
     * 3.数据分组
     * 4.划分个股时间窗口
     * 5.个股窗口数据处理
     * 6.划分板块时间窗口
     * 7.板块窗口数据处理
     * 8.转换并封装数据
     * 9.写入HDFS
     */
    @Override
    public void process(DataStream<CleanBean> waterData) {

        //1.设置HDFS存储路径
        BucketingSink<String> bucketingSink = new BucketingSink<>(QuotConfig.config.getProperty("sector.sec.hdfs.path"));
        //2.设置数据文件参数
        // （大小、分区、格式、前缀、后缀）
        bucketingSink.setBatchSize(Long.valueOf(QuotConfig.config.getProperty("hdfs.batch")));
        bucketingSink.setWriter(new StringWriter<>());
        bucketingSink.setBucketer(new DateTimeBucketer<>(QuotConfig.config.getProperty("hdfs.bucketer")));
        bucketingSink.setInProgressPrefix("sector-");
        bucketingSink.setPendingPrefix("sector2-");
        bucketingSink.setInProgressSuffix(".txt");
        bucketingSink.setPendingSuffix(".txt");


        waterData.keyBy(new KeyFunction())
                .timeWindow(Time.minutes(1))
                .apply(new MinStockWindowFunction())
                .timeWindowAll(Time.minutes(1))
                .apply(new SectorWindowFunction())
                .map(new SecterPutHdfsMap())
                .addSink(bucketingSink);

    }
}
