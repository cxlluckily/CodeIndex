package me.iroohom.task;

import me.iroohom.bean.CleanBean;
import me.iroohom.bean.WarnAmplitudeBean;
import me.iroohom.bean.WarnBaseBean;
import me.iroohom.inter.ProcessDataCepInterface;
import me.iroohom.inter.ProcessDataInterface;
import me.iroohom.mail.MailSend;
import me.iroohom.util.RedisUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import redis.clients.jedis.JedisCluster;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: AmplitudTask
 * @Author: Roohom
 * @Function: 振幅实时告警处理任务函数
 * @Date: 2020/11/6 20:25
 * @Software: IntelliJ IDEA
 */
public class AmplitudeTask implements ProcessDataCepInterface {

    /**
     * 总体开发步骤：
     * 1.数据转换
     * 2.初始化表执行环境
     * 3.注册表（流）
     * 4.sql执行
     * 5.表转流
     * 6.模式匹配,比较阀值
     * 7.查询数据
     * 8.发送告警邮件
     */
    @Override
    public void process(DataStream<CleanBean> waterData, StreamExecutionEnvironment env) {
        SingleOutputStreamOperator<WarnBaseBean> mapData = waterData.map(new MapFunction<CleanBean, WarnBaseBean>() {
            @Override
            public WarnBaseBean map(CleanBean value) throws Exception {
                // secCode、preClosePrice、highPrice、lowPrice、closePrice、eventTime
                return new WarnBaseBean(
                        value.getSecCode(),
                        value.getPreClosePrice(),
                        value.getMaxPrice(),
                        value.getMinPrice(),
                        value.getTradePrice(),
                        value.getEventTime()
                );
            }
        });

        //初始化表执行环境
        StreamTableEnvironment tablEnv = TableEnvironment.getTableEnvironment(env);

        //注册表 注意最后的字段要加 rowtime
        tablEnv.registerDataStream("tbl", mapData, "secCode,preClosePrice,highPrice,lowPrice,eventTime.rowtime");

        //sql执行
        String sql = "select secCode,preClosePrice,max(highPrice) as highPrice ," +
                " min(lowPrice) as lowPrice  from tbl group by secCode,preClosePrice,tumble(eventTime,interval '2' second)";

        Table table = tablEnv.sqlQuery(sql);
        DataStream<WarnAmplitudeBean> appendStream = tablEnv.toAppendStream(table, WarnAmplitudeBean.class);

        //取出redis振幅预警数值
        JedisCluster jedisCluster = RedisUtil.getJedisCluster();
        String threshold = jedisCluster.hget("quot", "amplitude");
        Pattern<WarnAmplitudeBean, WarnAmplitudeBean> pattern = Pattern.<WarnAmplitudeBean>begin("begin")
                .where(new SimpleCondition<WarnAmplitudeBean>() {
                    @Override
                    public boolean filter(WarnAmplitudeBean value) throws Exception {
                        //计算出振幅 最高价减最低价 除以昨日收盘价
                        BigDecimal amplitude = (value.getHighPrice().subtract(value.getLowPrice())).divide(value.getPreClosePrice(), 2, RoundingMode.HALF_UP);
                        return amplitude.compareTo(new BigDecimal(threshold)) > 0;
                    }
                });


        PatternStream<WarnAmplitudeBean> cep = CEP.pattern(appendStream.keyBy(WarnAmplitudeBean::getSecCode), pattern);
        cep.select(new PatternSelectFunction<WarnAmplitudeBean, Object>() {
            @Override
            public Object select(Map<String, List<WarnAmplitudeBean>> pattern) throws Exception {
                List<WarnAmplitudeBean> begin = pattern.get("begin");
                if (begin != null && begin.size() > 0) {
                    MailSend.send(begin.toString());
                }
                return begin;
            }
        });

        waterData.print("告警数据：");
    }
}
