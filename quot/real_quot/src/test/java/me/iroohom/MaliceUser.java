package me.iroohom;

import me.iroohom.bean.Message;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: MaliceUser
 * @Author: Roohom
 * @Function: 识别输入敏感词汇的用户
 * @Date: 2020/11/4 10:36
 * @Software: IntelliJ IDEA
 */
public class MaliceUser {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Message> source = env.fromCollection(Arrays.asList(
                new Message("1", "TMD", 1558430842000L),//2019-05-21 17:27:22
                new Message("1", "TMD", 1558430843000L),//2019-05-21 17:27:23
                new Message("1", "TMD", 1558430845000L),//2019-05-21 17:27:25
                new Message("1", "TMD", 1558430850000L),//2019-05-21 17:27:30
                new Message("1", "TMD", 1558430851000L),//2019-05-21 17:27:30
                new Message("2", "TMD", 1558430851000L),//2019-05-21 17:27:31
                new Message("1", "TMD", 1558430852000L)//2019-05-21 17:27:32

        )).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Message>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Message element) {
                return element.getEventTime();
            }
        });

        //定义规则模式
        Pattern<Message, Message> pattern = Pattern.<Message>begin("begin")
                .where(new IterativeCondition<Message>() {
                    @Override
                    public boolean filter(Message value, Context<Message> ctx) throws Exception {
                        return value.getMsg().equals("TMD");
                    }
                })
                //匹配5次
//                .times(5)
                .oneOrMore()
                //设置窗口时间
                .within(Time.seconds(10));

        PatternStream<Message> cep = CEP.pattern(source.keyBy(Message::getUserId), pattern);

        cep.select(new PatternSelectFunction<Message, List<Message>>() {
            @Override
            public List<Message> select(Map<String, List<Message>> pattern) throws Exception {
                List<Message> begin = pattern.get("begin");
                return begin;
            }
        }).print();

        env.execute();

    }
}
