package me.iroohom;

import me.iroohom.warn.*;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @ClassName: RackWarn
 * @Author: Roohom
 * @Function: 机架10秒内连续连两次上报的温度超过阈值 20秒内连续两次匹配警告并且温度超过了第一次的温度就报警
 * @Date: 2020/11/6 22:59
 * @Software: IntelliJ IDEA
 */
public class RackWarn {
    /**
     * 1.获取流处理执行环境
     * 2.设置事件时间
     * 3.加载数据源，接收监视数据,设置提取时间
     * 4.定义匹配模式，设置预警匹配规则，警告：10s内连续两次超过阀值
     * 5.生成匹配模式流（分组）
     * 6.数据处理,生成警告数据
     * 7.二次定义匹配模式，告警：20s内连续两次匹配警告
     * 8.二次生成匹配模式流（分组）
     * 9.数据处理生成告警信息flatSelect，返回类型
     * 10.数据打印(警告和告警)
     * 11.触发执行
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //加载数据源，接收见识数据，设置提取处理时间
        SingleOutputStreamOperator<MonitoringEvent> source = env.addSource(new MonitoringEventSource())
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        //定义匹配模式 10秒内连续两次温度超过阈值
        Pattern<MonitoringEvent, TemperatureEvent> pattern = Pattern.<MonitoringEvent>begin("begin")
                .subtype(TemperatureEvent.class)
                .where(new SimpleCondition<TemperatureEvent>() {
                    @Override
                    public boolean filter(TemperatureEvent value) throws Exception {
                        return value.getTemperature() > 100;
                    }
                }).next("next")
                .subtype(TemperatureEvent.class)
                .where(new SimpleCondition<TemperatureEvent>() {
                    @Override
                    public boolean filter(TemperatureEvent value) throws Exception {
                        return value.getTemperature() > 100;
                    }
                })
                .within(Time.seconds(10));

        //生成匹配模式流(分组)
        PatternStream<MonitoringEvent> cep = CEP.pattern(source.keyBy(MonitoringEvent::getRackID), pattern);
        SingleOutputStreamOperator<TemperatureWarning> warnData = cep.select(new PatternSelectFunction<MonitoringEvent, TemperatureWarning>() {
            @Override
            public TemperatureWarning select(Map<String, List<MonitoringEvent>> pattern) throws Exception {
                //获取模式匹配到的两条数据
                TemperatureEvent begin = (TemperatureEvent) pattern.get("begin").get(0);
                TemperatureEvent next = (TemperatureEvent) pattern.get("next").get(0);
                return new TemperatureWarning(begin.getRackID(), (begin.getTemperature() + next.getTemperature()) / 2);
            }
        });

        Pattern<TemperatureWarning, TemperatureWarning> patternAlert = Pattern.<TemperatureWarning>begin("begin")
                .next("next").within(Time.seconds(20));

        PatternStream<TemperatureWarning> cepAlert = CEP.pattern(warnData.keyBy(TemperatureWarning::getRackID), patternAlert);
        SingleOutputStreamOperator<Object> alertData = cepAlert.flatSelect(new PatternFlatSelectFunction<TemperatureWarning, Object>() {
            @Override
            public void flatSelect(Map<String, List<TemperatureWarning>> pattern, Collector<Object> out) throws Exception {
                //第二次的温度超过了第一次的就会报警
                TemperatureWarning begin = pattern.get("begin").get(0);
                TemperatureWarning next = pattern.get("next").get(0);
                if (next.getAverageTemperature() > begin.getAverageTemperature()) {
                    out.collect(new TemperatureAlert(begin.getRackID()));
                }
            }
        });

        warnData.print();
        alertData.print();

        env.execute();
    }
}
