package me.iroohom.task;

import me.iroohom.bean.CleanBean;
import me.iroohom.bean.SectorBean;
import me.iroohom.config.QuotConfig;
import me.iroohom.constant.KlineType;
import me.iroohom.function.KeyFunction;
import me.iroohom.function.MinStockWindowFunction;
import me.iroohom.function.SectorWindowFunction;
import me.iroohom.inter.ProcessDataInterface;
import me.iroohom.map.SectorKlineMap;
import me.iroohom.sink.SinkMysql;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

/**
 * @ClassName: SectorKlineTask
 * @Author: Roohom
 * @Function: 板块K线 Task
 * @Date: 2020/11/5 10:02
 * @Software: IntelliJ IDEA
 */
public class SectorKlineTask implements ProcessDataInterface {

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
        SingleOutputStreamOperator<SectorBean> applyData = waterData.keyBy(new KeyFunction())
                .timeWindow(Time.minutes(1))
                .apply(new MinStockWindowFunction())
                .timeWindowAll(Time.minutes(1))
                //获取板块数据
                .apply(new SectorWindowFunction());
        String sql = "replace into %s values(?,?,?,?,?,?,?,?,?,?,?,?,?)";

        //日 周 月 K线数据写入
        //日K数据
        applyData.map(new SectorKlineMap(KlineType.DAYK.getType(), KlineType.DAYK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(2).toString();
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.sector.sql.day.table"))));

        //周K数据
        applyData.map(new SectorKlineMap(KlineType.WEEKK.getType(), KlineType.WEEKK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(2).toString();
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.sector.sql.week.table"))));


        //月K数据
        applyData.map(new SectorKlineMap(KlineType.MONTHK.getType(), KlineType.MONTHK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(2).toString();
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.sector.sql.month.table"))));

    }
}
