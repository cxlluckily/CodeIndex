package me.iroohom.job;

import me.iroohom.avro.AvroDeserializationSchema;
import me.iroohom.avro.SseAvro;
import me.iroohom.avro.SzseAvro;
import me.iroohom.bean.CleanBean;
import me.iroohom.config.QuotConfig;
import me.iroohom.map.SseMap;
import me.iroohom.map.SzseMap;
import me.iroohom.util.QuotUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @ClassName: IndexStream
 * @Author: Roohom
 * @Function: 指数业务
 * @Date: 2020/11/15 20:58
 * @Software: IntelliJ IDEA
 */
public class IndexStream {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        //整合kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));
        properties.setProperty("group.id",QuotConfig.config.getProperty("group.id"));

        //实例化消费者对象
        FlinkKafkaConsumer011<SseAvro> sseConsumer = new FlinkKafkaConsumer011<>(QuotConfig.config.getProperty("sse.topic"), new AvroDeserializationSchema<>(QuotConfig.config.getProperty("sse.topic")), properties);
        FlinkKafkaConsumer011<SzseAvro> szseConsumer = new FlinkKafkaConsumer011<>(QuotConfig.config.getProperty("szse.topic"), new AvroDeserializationSchema<>(QuotConfig.config.getProperty("szse.tpopic")), properties);

        //设置从头消费
        sseConsumer.setStartFromEarliest();
        szseConsumer.setStartFromEarliest();

        //添加数据源
        DataStreamSource<SseAvro> sseSource = env.addSource(sseConsumer);
        DataStreamSource<SzseAvro> szseSource = env.addSource(szseConsumer);

        /**
         * 数据过滤
         */
        SingleOutputStreamOperator<SseAvro> sseFilterData = sseSource.filter(new FilterFunction<SseAvro>() {
            @Override
            public boolean filter(SseAvro value) throws Exception {
                return QuotUtil.checkTime(value) && QuotUtil.checkData(value);
            }
        });
        SingleOutputStreamOperator<SzseAvro> szseFilterData = szseSource.filter(new FilterFunction<SzseAvro>() {
            @Override
            public boolean filter(SzseAvro value) throws Exception {
                return QuotUtil.checkTime(value) && QuotUtil.checkData(value);
            }
        });

        //数据转换和合并
        DataStream<CleanBean> unionData = sseFilterData.map(new SseMap()).union(szseSource.map(new SzseMap()));

        //数据过滤 得到个股数据
        SingleOutputStreamOperator<CleanBean> indexData = unionData.filter(new FilterFunction<CleanBean>() {
            @Override
            public boolean filter(CleanBean value) throws Exception {
                return QuotUtil.isIndex(value);
            }
        });

        //设置水印
        SingleOutputStreamOperator<CleanBean> watermarksData = indexData.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CleanBean>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(CleanBean element) {
                return element.getEventTime();
            }
        });

        new IndexTask()


    }
}
