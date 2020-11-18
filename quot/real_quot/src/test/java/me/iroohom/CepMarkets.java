package me.iroohom;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import me.iroohom.bean.Product;
import me.iroohom.util.RedisUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.table.expressions.OverCall;
import org.apache.flink.table.expressions.VarPop;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @ClassName: CepMarkets
 * @Author: Roohom
 * @Function: 实时预警 市场监控 如果商品售价在1分钟之内有连续两次超过预定商品价格阀值就发送告警信息。
 * @Date: 2020/11/6 18:11
 * @Software: IntelliJ IDEA
 */
public class CepMarkets {
    /**
     * 1.获取流处理执行环境
     * 2.设置事件时间、并行度
     * 3.整合kafka
     * 4.数据转换
     * 5.process获取bean,设置status，并设置水印时间
     * 6.定义匹配模式，设置时间长度
     * 7.匹配模式（分组）
     * 8.查询告警数据
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        ;
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node01:9092");
        properties.setProperty("group.id", "cep");

        FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<>("cep", new SimpleStringSchema(), properties);
        //从头消费
        kafkaConsumer.setStartFromEarliest();

        //加载数据源
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        SingleOutputStreamOperator<Product> mapData = source.map(new MapFunction<String, Product>() {
            @Override
            public Product map(String value) throws Exception {
                //将字符串解析成Json
                JSONObject jsonObject = JSON.parseObject(value);
                return new Product(
                        jsonObject.getLongValue("goodsId"),
                        jsonObject.getDouble("goodsPrice"),
                        jsonObject.getString("goodsName"),
                        jsonObject.getString("alias"),
                        jsonObject.getLongValue("orderTime"),
                        false
                );
            }
        });
        //source获取bean 设置status 并设置水印时间
        SingleOutputStreamOperator<Product> productData = mapData.process(new ProcessFunction<Product, Product>() {
            JedisCluster jedisCluster = null;

            /**
             * 初始化方法，获取Redis连接
             * @param parameters
             * @throws Exception
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                jedisCluster = RedisUtil.getJedisCluster();
            }

            /**
             * 将商品的价格和阈值数据进行比较，设置status
             * @param value Product对象
             * @param ctx 上下文
             * @param out 删除值
             * @throws Exception 异常pass
             */
            @Override
            public void processElement(Product value, Context ctx, Collector<Product> out) throws Exception {
                //获取Redis阈值数据
                String threshold = jedisCluster.hget("product", value.getGoodsName());
                //如果这个商品的价格比阈值大，就将状态设置为true
                if (value.getGoodsPrice() > Double.parseDouble(threshold)) {
                    value.setStatus(true);
                }
                out.collect(value);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Product>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Product element) {
                return element.getOrderTime();
            }
        });


        //定义匹配模式 如果商品售价在1分钟之内有连续两次超过预定商品价格阀值就发送告警信息
        Pattern<Product, Product> pattern = Pattern.<Product>begin("begin")
                .where(new SimpleCondition<Product>() {
                    @Override
                    public boolean filter(Product value) throws Exception {
                        return value.getStatus();
                    }
                })
                //因为是连续两次 所以用next
                .next("next")
                .where(new SimpleCondition<Product>() {
                    @Override
                    public boolean filter(Product value) throws Exception {
                        return value.getStatus();
                    }
                })
                //在一分钟之内
                .within(Time.minutes(1));


        PatternStream<Product> cep = CEP.pattern(productData.keyBy(Product::getGoodsId), pattern);

        cep.select(new PatternSelectFunction<Product, Object>() {
            @Override
            public Object select(Map<String, List<Product>> pattern) throws Exception {
                List<Product> next = pattern.get("next");
                return next;
            }
        }).print();

        env.execute();
    }
}
