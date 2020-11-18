package me.iroohom.job;

import me.iroohom.avro.AvroDeserializationSchema;
import me.iroohom.avro.SseAvro;
import me.iroohom.avro.SzseAvro;
import me.iroohom.bean.CleanBean;
import me.iroohom.config.QuotConfig;
import me.iroohom.map.SseMap;
import me.iroohom.map.SzseMap;
import me.iroohom.task.StockIncreaseTask;
import me.iroohom.task.StockMinHdfsTask;
import me.iroohom.task.StockMinuteTask;
import me.iroohom.task.StockSecTask;
import me.iroohom.util.QuotUtil;
import org.apache.flink.api.common.functions.FilterFunction;
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
 * @ClassName: SocketStream
 * @Author: Roohom
 * @Function: 主类 个股业务
 * @Date: 2020/10/31 21:40
 * @Software: IntelliJ IDEA
 */
public class StockStream {

    /**
     * 个股总体开发步骤：
     * 1.创建StockStream单例对象，创建main方法
     * 2.获取流处理执行环境
     * 3.设置事件时间、并行度
     * 4.设置检查点机制
     * 5.设置重启机制
     * 6.整合Kafka(新建反序列化类)
     * 7.数据过滤（时间和null字段）
     * 8.数据转换、合并
     * 9.过滤个股数据
     * 10.设置水位线
     * 11.业务数据处理
     * 12.触发执行
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置时间时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置并行度，在此设置为1，便于开发和测试
        env.setParallelism(1);

        //在开发时，检查点开了没啥用，所以关闭，但生产环境一定是需要开启的
//        env.enableCheckpointing(5000L);
//        env.setStateBackend(new FsStateBackend("hdfs://node01:8020/checkpoint/stock"));
//        //设置强一致性
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //设置检查点制作失败，任务继续进行
//        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
//        //设置最大线程数
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        //任务取消的时候，保留检查点，需要手动删除老的检查点
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
//        //设置重启机制
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));
//
        //整合Kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));
        properties.setProperty("group.id", QuotConfig.config.getProperty("group.id"));

        FlinkKafkaConsumer011<SseAvro> sseKafkaConsumer = new FlinkKafkaConsumer011<SseAvro>(QuotConfig.config.getProperty("sse.topic"), new AvroDeserializationSchema(QuotConfig.config.getProperty("sse.topic")), properties);
        FlinkKafkaConsumer011<SzseAvro> szseKafkaConsumer = new FlinkKafkaConsumer011<SzseAvro>(QuotConfig.config.getProperty("szse.topic"), new AvroDeserializationSchema(QuotConfig.config.getProperty("szse.topic")), properties);

        //设置从头消费
        sseKafkaConsumer.setStartFromEarliest();
        szseKafkaConsumer.setStartFromEarliest();


        //添加数据源
        DataStreamSource<SseAvro> sseSource = env.addSource(sseKafkaConsumer);
        DataStreamSource<SzseAvro> szseSource = env.addSource(szseKafkaConsumer);
//        szseSource.print();
//        sseSource.print();
        /**
         * 数据过滤
         * 保证数据接收时间在开闭市时间区间之间
         * 过滤数据中最高价、最低价、开盘价和收盘价为0的数据,保证数据都不为0
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

        //数据转换 合并
        DataStream<CleanBean> unionData = sseFilterData.map(new SseMap()).union(szseFilterData.map(new SzseMap()));

        //过滤得到个股数据
        SingleOutputStreamOperator<CleanBean> stockData = unionData.filter(new FilterFunction<CleanBean>() {
            @Override
            public boolean filter(CleanBean value) throws Exception {
                return QuotUtil.isStock(value);
            }
        });

        SingleOutputStreamOperator<CleanBean> watermarksData = stockData.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CleanBean>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(CleanBean element) {
                return element.getEventTime();
            }
        });


        watermarksData.print("水位线数据:  ");

        /**
         * 1.秒级行情(5s)(掌握)
         * 2.分时行情（60s）（掌握）
         * 3.分时行情备份（掌握）
         * 4.个股涨幅榜（60s）
         */
//
        //秒级行情，写入Hbase TODO: 秒级SinkHbase已测试
        new StockSecTask().process(watermarksData);
        //分时行情（60s），数据写入Druid和Kafka TODO:分时SinkDruid SinkKafka 已测试
//        new StockMinuteTask().process(watermarksData);
        //分时行情备份至HDFS TODO:分时备份SinkHDFS已测试
//        new StockMinHdfsTask().process(watermarksData);
        //个股涨幅榜，数据写入Kafka  TODO:个股涨幅 SinkKafka 已测试
//        new StockIncreaseTask().process(watermarksData);

        env.execute("Stock Stream");
    }
}
