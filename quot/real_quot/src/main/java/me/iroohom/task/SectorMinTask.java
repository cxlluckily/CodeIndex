package me.iroohom.task;

import com.alibaba.fastjson.JSON;
import me.iroohom.bean.CleanBean;
import me.iroohom.bean.SectorBean;
import me.iroohom.config.QuotConfig;
import me.iroohom.function.KeyFunction;
import me.iroohom.function.MinStockWindowFunction;
import me.iroohom.function.SectorWindowFunction;
import me.iroohom.inter.ProcessDataInterface;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * @ClassName: SectorMinTask
 * @Author: Roohom
 * @Function: 分时行情Task
 * @Date: 2020/11/4 17:20
 * @Software: IntelliJ IDEA
 */
public class SectorMinTask implements ProcessDataInterface {

    /**
     * 开发步骤：
     * 1.数据分组
     * 2.划分个股时间窗口
     * 3.个股分时数据处理
     * 4.划分板块时间窗口
     * 5.板块分时数据处理
     * 6.数据转换成字符串
     * 7.数据写入kafka
     */
    @Override
    public void process(DataStream<CleanBean> waterData) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));
        FlinkKafkaProducer011<String> kafkaProducer = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("sse.sector.topic"), new SimpleStringSchema(), properties);

        waterData.keyBy(new KeyFunction())
                .timeWindow(Time.minutes(1))
                .apply(new MinStockWindowFunction())
                .timeWindowAll(Time.minutes(1))
                .apply(new SectorWindowFunction())
                .map(new MapFunction<SectorBean, String>() {
                    @Override
                    public String map(SectorBean value) throws Exception {
                        return JSON.toJSONString(value);
                    }
                }).addSink(kafkaProducer);
    }
}
