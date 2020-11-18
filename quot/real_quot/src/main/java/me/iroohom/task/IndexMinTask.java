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
 * @ClassName: IndexMinTask
 * @Author: Roohom
 * @Function: 指数分时行情处理任务
 * @Date: 2020/11/5 13:46
 * @Software: IntelliJ IDEA
 */
public class IndexMinTask implements ProcessDataInterface {
    /**
     * 开发步骤：
     * 1.定义侧边流
     * 2.数据分组
     * 3.划分时间窗口
     * 4.分时数据处理（新建分时窗口函数）
     * 5.数据分流
     * 6.数据分流转换
     * 7.分表存储(写入kafka)
     */


    @Override
    public void process(DataStream<CleanBean> waterData) {
        //定义侧边流
        OutputTag<IndexBean> indexSzseOpt = new OutputTag<>("indexSzseOpt", TypeInformation.of(IndexBean.class));


        SingleOutputStreamOperator<IndexBean> processData = waterData
                .keyBy(new KeyFunction())
                // 3.划分时间窗口
                .timeWindow(Time.minutes(1))
                //4.分时数据处理（新建分时窗口函数）
                .apply(new MinIndexWindowFunction())
                //5.数据分流
                .process(new ProcessFunction<IndexBean, IndexBean>() {
                    @Override
                    public void processElement(IndexBean value, Context ctx, Collector<IndexBean> out) throws Exception {
                        if (value.getSource().equals("sse")) {
                            out.collect(value);
                        } else {
                            ctx.output(indexSzseOpt, value);
                        }
                    }
                });



        //数据分流转换
        //沪市数据
        SingleOutputStreamOperator<String> sseData = processData.map(new MapFunction<IndexBean, String>() {
            @Override
            public String map(IndexBean value) throws Exception {
                return JSON.toJSONString(value);
            }
        });
        //深市数据
        SingleOutputStreamOperator<String> szseData = processData.getSideOutput(indexSzseOpt)
                .map(new MapFunction<IndexBean, String>() {
                    @Override
                    public String map(IndexBean value) throws Exception {
                        return JSON.toJSONString(value);
                    }
                });

        //分表存储（写入Kafka）
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));

        //sse
        FlinkKafkaProducer011<String> sseKafkaProducer = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("sse.index.topic"), new SimpleStringSchema(), properties);
        //szse
        FlinkKafkaProducer011<String> szseKafkaProducer = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("szse.index.topic"), new SimpleStringSchema(), properties);

        //写入KAFKA
        sseData.addSink(sseKafkaProducer);
        szseData.addSink(szseKafkaProducer);

    }
}
