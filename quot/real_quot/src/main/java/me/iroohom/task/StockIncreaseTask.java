package me.iroohom.task;

import com.alibaba.fastjson.JSON;
import me.iroohom.bean.CleanBean;
import me.iroohom.bean.StockIncrBean;
import me.iroohom.config.QuotConfig;
import me.iroohom.function.KeyFunction;
import me.iroohom.function.StockIncrWindowFunction;
import me.iroohom.inter.ProcessDataInterface;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * @ClassName: StockIncreaseTask
 * @Author: Roohom
 * @Function: 个股涨幅榜任务
 * @Date: 2020/11/2 16:04
 * @Software: IntelliJ IDEA
 */
public class StockIncreaseTask implements ProcessDataInterface {


    /**
     * 开发步骤：
     * 1.数据分组
     * 2.划分时间窗口
     * 3.创建bean对象
     * 4.分时数据处理（新建分时窗口函数）
     * 5.数据转换成字符串
     * 6.数据存储(单表)
     * 注意：
     * 1.数据也是插入到druid
     * 2.要新建topic
     * 3.开启摄取数据的进程
     * 4.单表存储沪深两市数据
     * 5.基于分钟级数据，进行业务开发
     */
    @Override
    public void process(DataStream<CleanBean> waterData) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",QuotConfig.config.getProperty("bootstrap.servers"));
        FlinkKafkaProducer011<String> kafkaProducer = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("stock.increase.topic"), new SimpleStringSchema(), properties);



        waterData.keyBy(new KeyFunction())
                //划分时间窗口
                .timeWindow(Time.minutes(1))
                //分时数据处理
                .apply(new StockIncrWindowFunction())
                //数据转换成字符串
                .map(new MapFunction<StockIncrBean,String>() {
                    @Override
                    public String map(StockIncrBean value) throws Exception {
                        return JSON.toJSONString(value);
                    }
                })
                //数据存储
                .addSink(kafkaProducer);
    }
}
