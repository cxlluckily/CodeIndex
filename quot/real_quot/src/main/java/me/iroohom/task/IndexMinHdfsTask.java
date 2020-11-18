package me.iroohom.task;

import me.iroohom.bean.CleanBean;
import me.iroohom.config.QuotConfig;
import me.iroohom.function.KeyFunction;
import me.iroohom.function.MinIndexWindowFunction;
import me.iroohom.inter.ProcessDataInterface;
import me.iroohom.map.IndexPutHdfsMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

/**
 * @ClassName: IndexMinHdfsTask
 * @Author: Roohom
 * @Function: 指数分时行情备份至HDFS任务
 * @Date: 2020/11/5 17:17
 * @Software: IntelliJ IDEA
 */
public class IndexMinHdfsTask implements ProcessDataInterface {
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
     */

    @Override
    public void process(DataStream<CleanBean> waterData) {
        BucketingSink<String> bucketingSink = new BucketingSink<>(QuotConfig.config.getProperty("index.sec.hdfs.path"));
        bucketingSink.setBatchSize(Long.valueOf(QuotConfig.config.getProperty("hdfs.batch")));
        bucketingSink.setBucketer(new DateTimeBucketer<>(QuotConfig.config.getProperty("hdfs.bucketer")));
        //字符串格式
        bucketingSink.setWriter(new StringWriter<>());
        bucketingSink.setInProgressPrefix("index-");
        bucketingSink.setPendingPrefix("index2-");
        bucketingSink.setInProgressSuffix(".txt");
        bucketingSink.setPendingSuffix(".txt");

        //数据分组
        waterData.keyBy(new KeyFunction())
                .timeWindow(Time.seconds(1))
                .apply(new MinIndexWindowFunction())
                .map(new IndexPutHdfsMap())
                .addSink(bucketingSink)
                .setParallelism(1);



    }
}
