package me.iroohom.task;

import me.iroohom.bean.CleanBean;
import me.iroohom.bean.WarnBaseBean;
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
import redis.clients.jedis.JedisCluster;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: UpdownTask
 * @Author: Roohom
 * @Function: 涨跌幅告警处理任务
 * @Date: 2020/11/6 20:57
 * @Software: IntelliJ IDEA
 */
public class UpdownTask implements ProcessDataInterface {
    /**
     * 开发步骤：
     * 1.数据转换map
     * 2.封装bean对象
     * 3.加载redis涨跌幅数据
     * 4.模式匹配
     * 5.获取匹配模式流数据
     * 6.查询数据
     * 7.发送告警邮件
     */
    @Override
    public void process(DataStream<CleanBean> waterData) {
        SingleOutputStreamOperator<WarnBaseBean> mapData = waterData.map(new MapFunction<CleanBean, WarnBaseBean>() {
            @Override
            public WarnBaseBean map(CleanBean value) throws Exception {
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


        //加载Redis涨跌幅数据
        JedisCluster jedisCluster = RedisUtil.getJedisCluster();
        BigDecimal up1 = new BigDecimal(jedisCluster.hget("quot", "upDown1"));
        BigDecimal up2 = new BigDecimal(jedisCluster.hget("quot", "upDown2"));


        Pattern<WarnBaseBean, WarnBaseBean> pattern = Pattern.<WarnBaseBean>begin("begin")
                .where(new SimpleCondition<WarnBaseBean>() {
                    @Override
                    public boolean filter(WarnBaseBean value) throws Exception {
                        //计算出涨跌幅
                        BigDecimal upDown = value.getClosePrice().subtract(value.getPreClosePrice()).divide(value.getPreClosePrice(), 2, RoundingMode.HALF_UP);
                        return upDown.compareTo(up1) > 0 && upDown.compareTo(up2) < 0;
                    }
                });

        PatternStream<WarnBaseBean> cep = CEP.pattern(mapData.keyBy(WarnBaseBean::getSecCode), pattern);

        cep.select(new PatternSelectFunction<WarnBaseBean, Object>() {
            @Override
            public Object select(Map<String, List<WarnBaseBean>> pattern) throws Exception {
                List<WarnBaseBean> begin = pattern.get("begin");
                if (begin != null && begin.size() > 0) {
                    MailSend.send(begin.toString());
                }
                return begin;
            }
        }).print();
    }
}
