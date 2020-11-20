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
 * @Date 2020/11/3
 */
public class SectorKlineTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {

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
        //1.数据分组
        SingleOutputStreamOperator<SectorBean> applyData = waterData.keyBy(new KeyFunction())
                .timeWindow(Time.minutes(1))
                //获取个股数据
                .apply(new MinStockWindowFunction())
                //获取板块数据
                .timeWindowAll(Time.minutes(1))
                .apply(new SectorWindowFunction());

        //4.编写插入sql,%s ：格式化转换符
        String sql = "replace into %s values(?,?,?,?,?,?,?,?,?,?,?,?,?)";

        //5.（日、周、月）K线数据写入
        //日
        applyData.map(new SectorKlineMap(KlineType.DAYK.getType(),KlineType.DAYK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(2).toString();
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.sector.sql.day.table"))));
        //周
        applyData.map(new SectorKlineMap(KlineType.WEEKK.getType(),KlineType.WEEKK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(2).toString();
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.sector.sql.week.table"))));
        //月
        applyData.map(new SectorKlineMap(KlineType.MONTHK.getType(),KlineType.MONTHK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(2).toString();
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.sector.sql.month.table"))));
    }
}
