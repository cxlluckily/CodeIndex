package me.iroohom.task;

import me.iroohom.bean.CleanBean;
import me.iroohom.bean.TurnoverRateBean;
import me.iroohom.inter.ProcessDataInterface;
import me.iroohom.mail.MailSend;
import me.iroohom.util.DbUtil;
import me.iroohom.util.RedisUtil;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.JedisCluster;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: TurnoverRateTask
 * @Author: Roohom
 * @Function: 实时告警业务换手率任务
 * @Date: 2020/11/6 21:12
 * @Software: IntelliJ IDEA
 */
public class TurnoverRateTask implements ProcessDataInterface {
    /**
     * 总体开发步骤：
     * 1.创建bean对象
     * 2.数据转换process
     * (1)加载mysql流通股本数据
     * (2)封装bean对象数据
     * 3.加载redis换手率数据
     * 4.模式匹配
     * 5.查询数据
     * 6.发送告警邮件
     * 7.数据打印
     */
    @Override
    public void process(DataStream<CleanBean> waterData) {
        //这里使用process 而不是用map的原因是ProcessFunction继承自AbstractFunction 拥有open 和close初始化方法 这里只用open
        SingleOutputStreamOperator<TurnoverRateBean> processData = waterData.process(new ProcessFunction<CleanBean, TurnoverRateBean>() {
            Map<String, Map<String, Object>> map;

            /**
             * 初始化方法，用来连接MySQL获取流通股本数据
             * @param parameters
             * @throws Exception
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                String sql = "SELECT * FROM bdp_sector_stock";

                //此Map的键是sec_code 值为一行数据的Map
                map = DbUtil.query("sec_code", sql);
            }

            @Override
            public void processElement(CleanBean value, Context ctx, Collector<TurnoverRateBean> out) throws Exception {
                String secCode = value.getSecCode();
                //从板块成分股表中根据代码查出对应的一行数据
                Map<String, Object> negCapMap = map.get(secCode);

                if (negCapMap != null) {
                    //查出流通股本
                    BigDecimal negoCap = new BigDecimal(negCapMap.get("nego_cap").toString());

                    //封装bean输出
                    out.collect(new TurnoverRateBean(
                            value.getSecCode(),
                            value.getSecName(),
                            value.getTradePrice(),
                            value.getTradeVolume(),
                            negoCap
                    ));
                }
            }
        });


        //加载Redis还手率数据
        JedisCluster jedisCluster = RedisUtil.getJedisCluster();
        BigDecimal threshold = new BigDecimal(jedisCluster.hget("quot", "turnoverRate"));


        Pattern<TurnoverRateBean, TurnoverRateBean> pattern = Pattern.<TurnoverRateBean>begin("begin")
                .where(new SimpleCondition<TurnoverRateBean>() {
                    @Override
                    public boolean filter(TurnoverRateBean value) throws Exception {
                        //计算出换手率 再与告警阈值进行比较
                        BigDecimal turnoverRate = new BigDecimal(value.getTradeVol()).divide(value.getNegoCap(), 2, RoundingMode.HALF_UP);
                        return turnoverRate.compareTo(threshold) > 0;
                    }
                });


        PatternStream<TurnoverRateBean> cep = CEP.pattern(processData.keyBy(TurnoverRateBean::getSecCode), pattern);

        cep.select(new PatternSelectFunction<TurnoverRateBean, Object>() {
            @Override
            public Object select(Map<String, List<TurnoverRateBean>> pattern) throws Exception {
                List<TurnoverRateBean> begin = pattern.get("begin");
                if (begin!=null&&begin.size()>0)
                {
                    MailSend.send(begin.toString());
                }
                return begin;
            }
        }).print();

    }
}
