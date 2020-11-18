package me.iroohom;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/**
 * @ClassName: ConsecutiveDemo
 * @Author: Roohom
 * @Function: 组合模式 从数据源中依次提取"c","a","b"元素
 * @Date: 2020/11/6 17:46
 * @Software: IntelliJ IDEA
 */
public class ConsecutiveDemo {
    /**
     * 开发步骤（java）：
     * 1.获取流处理执行环境
     * 2.设置但并行度
     * 3.加载数据源
     * 4.设置匹配模式，匹配"c","a","b"
     * 多次匹配"a"：组合模式
     * 5.匹配数据提取Tuple3
     * 6.数据打印
     * 7.触发执行
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.fromElements("c", "d", "a", "a", "a", "d", "a", "b");


        //设置匹配模式，匹配c a b
        Pattern<String, String> pattern = Pattern.<String>begin("begin")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.equals("c");
                    }
                })
                //松散 不严格临近
                .followedBy("middle")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.equals("a");
                    }
                })
                //匹配一次或者多次a
                .oneOrMore()
                //允许组合
//                .allowCombinations()
                .followedBy("end")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.equals("b");
                    }
                });


        //匹配数据提取
        PatternStream<String> cep = CEP.pattern(source, pattern);
        cep.select(new PatternSelectFunction<String, Object>() {
            @Override
            public Object select(Map<String, List<String>> pattern) throws Exception {
                List<String> begin = pattern.get("begin");
                List<String> middle = pattern.get("middle");
                List<String> end = pattern.get("end");
                return Tuple3.of(begin,middle,end);
            }
        }).print();

        env.execute();

    }
}
