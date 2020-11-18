package me.iroohom.task;

import com.alibaba.fastjson.JSON;
import me.iroohom.bean.CleanBean;
import me.iroohom.bean.IndexBean;
import me.iroohom.config.QuotConfig;
import me.iroohom.function.KeyFunction;
import me.iroohom.function.MinIndexWindowFunction;
import me.iroohom.inter.ProcessDataInterface;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

/**
 * @ClassName: IndexTask
 * @Author: Roohom
 * @Function: 指数数据处理任务
 * @Date: 2020/11/15 21:32
 * @Software: IntelliJ IDEA
 */
public class IndexTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        //定义侧边流
        OutputTag<IndexBean> indexSzseOpt = new OutputTag<IndexBean>("indexSzseOpt", TypeInformation.of(IndexBean.class));

        SingleOutputStreamOperator<IndexBean> processData = waterData
                .keyBy(new KeyFunction())
                .timeWindow(Time.minutes(1))
                .apply(new MinIndexWindowFunction())
                .process(new ProcessFunction<IndexBean, IndexBean>() {
                    @Override
                    public void processElement(IndexBean value, Context ctx, Collector<IndexBean> out) throws Exception {
                        if ("sse".equals(value.getSource())) {
                            out.collect(value);
                        } else {
                            ctx.output(indexSzseOpt, value);
                        }
                    }
                });

        /**
         * 沪市数据分拣
         */
        SingleOutputStreamOperator<String> sseData = processData.map(new MapFunction<IndexBean, String>() {
            @Override
            public String map(IndexBean value) throws Exception {
                return JSON.toJSONString(value);
            }
        });

        /**
         * 深市数据
         */
        SingleOutputStreamOperator<String> szseData = processData.map(new MapFunction<IndexBean, String>() {
            @Override
            public String map(IndexBean value) throws Exception {
                return JSON.toJSONString(value);
            }
        });

        /**
         * 存入Kafka
         */
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));

        FlinkKafkaProducer011<String> sseKafkaProducer = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("sse.topic"), new SimpleStringSchema(), properties);
        FlinkKafkaProducer011<String> szseKafkaProducer = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("szse.topic"), new SimpleStringSchema(), properties);

        sseData.addSink(sseKafkaProducer);
        szseData.addSink(szseKafkaProducer);

    }
}
