package me.iroohom.task;

import me.iroohom.bean.CleanBean;
import me.iroohom.bean.IndexBean;
import me.iroohom.config.QuotConfig;
import me.iroohom.constant.KlineType;
import me.iroohom.function.KeyFunction;
import me.iroohom.function.MinIndexWindowFunction;
import me.iroohom.inter.ProcessDataInterface;
import me.iroohom.map.IndexKlineMap;
import me.iroohom.sink.SinkMysql;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

/**
 * @ClassName: IndexKlineTask
 * @Author: Roohom
 * @Function: 指数K线 写入MySQL
 * @Date: 2020/11/5 18:58
 * @Software: IntelliJ IDEA
 */
public class IndexKlineTask implements ProcessDataInterface {
    /**
     * 开发步骤：
     * 1.数据分组
     * 2.划分时间窗口
     * 3.数据处理
     * 4.编写插入sql
     * 5.（日、周、月）K线数据写入
     * 数据转换、分组
     * 数据写入mysql
     */
    @Override
    public void process(DataStream<CleanBean> waterData) {
        SingleOutputStreamOperator<IndexBean> applyData = waterData.keyBy(new KeyFunction())
                .timeWindow(Time.minutes(1))
                .apply(new MinIndexWindowFunction());

        //编写插入SQL K线数据是要替换的 所以用replace
        String sql = "replace into %s values(?,?,?,?,?,?,?,?,?,?,?,?,?)";

        //日K
        applyData.map(new IndexKlineMap(KlineType.DAYK.getType(), KlineType.DAYK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(2).toString();
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.index.sql.day.table"))));

        //周K
        applyData.map(new IndexKlineMap(KlineType.WEEKK.getType(), KlineType.WEEKK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(2).toString();
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.index.sql.week.table"))));

        //月K
        applyData.map(new IndexKlineMap(KlineType.MONTHK.getType(), KlineType.MONTHK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(2).toString();
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.index.sql.month.table"))));
    }
}
