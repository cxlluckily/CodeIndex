package me.iroohom;

import me.iroohom.bean.LoginEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: ConditionDemo
 * @Author: Roohom
 * @Function: 过滤条件 查询匹配用户登陆状态是fail，且失败次数大于8的数据
 * @Date: 2020/11/6 17:32
 * @Software: IntelliJ IDEA
 */
public class ConditionDemo {
    /**
     * 开发步骤（java）：
     * 1.获取流处理执行环境
     * 2.设置但并行度
     * 3.加载数据源
     * 4.设置匹配模式连续where，
     * 先匹配状态（多次），再匹配数量
     * 5.匹配数据提取，返回集合
     * 6.数据打印
     * 7.触发执行
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<LoginEvent> source = env.fromCollection(Arrays.asList(
                new LoginEvent("1", "192.168.0.1", "fail", 8),
                new LoginEvent("1", "192.168.0.2", "fail", 9),
                new LoginEvent("1", "192.168.0.3", "fail", 10),
                new LoginEvent("1", "192.168.0.4", "fail", 10),
                new LoginEvent("2", "192.168.10.10", "success", -1),
                new LoginEvent("3", "192.168.10.10", "fail", 5),
                new LoginEvent("3", "192.168.10.11", "fail", 6),
                new LoginEvent("4", "192.168.10.10", "fail", 6),
                new LoginEvent("4", "192.168.10.11", "fail", 7),
                new LoginEvent("4", "192.168.10.12", "fail", 8),
                new LoginEvent("5", "192.168.10.13", "success", 8),
                new LoginEvent("5", "192.168.10.14", "success", 9),
                new LoginEvent("5", "192.168.10.15", "success", 10),
                new LoginEvent("6", "192.168.10.16", "fail", 6),
                new LoginEvent("6", "192.168.10.17", "fail", 8),
                new LoginEvent("7", "192.168.10.18", "fail", 5),
                new LoginEvent("6", "192.168.10.19", "fail", 10),
                new LoginEvent("6", "192.168.10.18", "fail", 9)
        ));

        //设置匹配模式
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("begin")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return value.getStatus().equals("fail");
                    }
                })
//                .times(2)
                .oneOrMore()
                .until(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.getCount() > 8;
                    }
                });

        //匹配数据提取，返回
        PatternStream<LoginEvent> cep = CEP.pattern(source.keyBy(LoginEvent::getId), pattern);

        cep.select(new PatternSelectFunction<LoginEvent, Object>() {
            @Override
            public Object select(Map<String, List<LoginEvent>> pattern) throws Exception {
                List<LoginEvent> begin = pattern.get("begin");
                return begin;
            }
        }).print();

        env.execute();


    }
}
