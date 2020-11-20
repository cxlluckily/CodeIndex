package me.iroohom.job;

import me.iroohom.avro.AvroDeserializationSchema;
import me.iroohom.avro.SseAvro;
import me.iroohom.avro.SzseAvro;
import me.iroohom.bean.CleanBean;
import me.iroohom.config.QuotConfig;
import me.iroohom.map.SseMap;
import me.iroohom.map.SzseMap;
import me.iroohom.task.AmplitudeTask;
import me.iroohom.task.TurnoverRateTask;
import me.iroohom.task.UpdownTask;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * @ClassName: WarnStream
 * @Author: Roohom
 * @Function: 实时告警
 * @Date: 2020/11/6 20:06
 * @Software: IntelliJ IDEA
 */
public class WarnStream {
    /**
     * 1.创建WarnStream单例对象，创建main方法
     * 2.获取流处理执行环境
     * 3.设置事件时间、并行度
     * 4.设置检查点机制
     * 5.设置重启机制
     * 6.整合Kafka(新建反序列化类)
     * 7.数据过滤（时间和null字段）
     * 8.数据转换、合并
     * 9.过滤个股数据
     * 10.设置水位线
     * 11.业务数据处理：涨跌幅、振幅和换手率
     * 12.触发执行
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        //设置检查点和重启机制测试用不到，所以此处省略

        //整合KAFKA
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));
        properties.setProperty("group.id", QuotConfig.config.getProperty("group.id"));

        //kafka消费者对象
        FlinkKafkaConsumer011<SseAvro> ssekafkaConsumer = new FlinkKafkaConsumer011<SseAvro>(QuotConfig.config.getProperty("sse.topic"), new AvroDeserializationSchema<>(QuotConfig.config.getProperty("sse.topic")), properties);
        FlinkKafkaConsumer011<SzseAvro> szseKafkaConsumer = new FlinkKafkaConsumer011<>(QuotConfig.config.getProperty("szse.topic"), new AvroDeserializationSchema<>(QuotConfig.config.getProperty("szse.topic")), properties);

        ssekafkaConsumer.setStartFromEarliest();
        szseKafkaConsumer.setStartFromEarliest();

        DataStreamSource<SseAvro> sseSource = env.addSource(ssekafkaConsumer);
        DataStreamSource<SzseAvro> szseSource = env.addSource(szseKafkaConsumer);

        //数据过滤
        SingleOutputStreamOperator<SseAvro> sseFilterData = sseSource.filter(new FilterFunction<SseAvro>() {
            @Override
            public boolean filter(SseAvro value) throws Exception {
                return QuotUtil.checkData(value) && QuotUtil.checkTime(value);
            }
        });

        SingleOutputStreamOperator<SzseAvro> szseFilterData = szseSource.filter(new FilterFunction<SzseAvro>() {
            @Override
            public boolean filter(SzseAvro value) throws Exception {
                return QuotUtil.checkTime(value) && QuotUtil.checkData(value);
            }
        });


        //数据转换、合并
        //数据合并
        DataStream<CleanBean> unionData = sseFilterData.map(new SseMap()).union(szseFilterData.map(new SzseMap()));
        SingleOutputStreamOperator<CleanBean> filterData = unionData.filter(new FilterFunction<CleanBean>() {
            @Override
            public boolean filter(CleanBean value) throws Exception {
                //过滤个股数据
                return QuotUtil.isStock(value);
            }
        });

        //设置水位线
        SingleOutputStreamOperator<CleanBean> watermarksData = filterData.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CleanBean>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(CleanBean element) {
                return element.getEventTime();
            }
        });

        //打印测试观察
        watermarksData.print();

        //振幅告警 TODO:待测试
        new AmplitudeTask().process(watermarksData,env);
        //涨跌幅告警 TODO:待测试
        new UpdownTask().process(watermarksData);

        new TurnoverRateTask().process(watermarksData);
        env.execute();
    }

}
