package me.iroohom.task;

import me.iroohom.bean.CleanBean;
import me.iroohom.bean.StockBean;
import me.iroohom.config.QuotConfig;
import me.iroohom.constant.KlineType;
import me.iroohom.function.KeyFunction;
import me.iroohom.function.MinStockWindowFunction;
import me.iroohom.inter.ProcessDataInterface;
import me.iroohom.map.StockKlineMap;
import me.iroohom.sink.SinkMysql;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName: SectorKlineTask
 * @Author: Roohom
 * @Function: 个股K线数据
 * @Date: 2020/11/4 19:23
 * @Software: IntelliJ IDEA
 */
public class StockKlineTask implements ProcessDataInterface {

    /**
     * 开发步骤：
     * 1.新建侧边流分支（周、月）
     * 2.数据分组
     * 3.划分时间窗口
     * 4.数据处理
     * 5.分流、封装侧边流数据
     * 6.编写插入sql
     * 7.（日、周、月）K线数据写入
     * 数据转换
     * 数据分组
     * 数据写入mysql
     */

    @Override
    public void   process(DataStream<CleanBean> waterData) {

        //周K侧边流存储周K数据
        OutputTag<StockBean> weekOpt = new OutputTag<>("weekOpt", TypeInformation.of(StockBean.class));
        //月K侧边流，存储月K数据
        OutputTag<StockBean> monthOpt = new OutputTag<>("monthOpt", TypeInformation.of(StockBean.class));
        //数据分组
        SingleOutputStreamOperator<StockBean> processData = waterData.keyBy(new KeyFunction())
                .timeWindow(Time.minutes(1))
                .apply(new MinStockWindowFunction())
                //数据处理
                .process(new ProcessFunction<StockBean, StockBean>() {
                    @Override
                    public void processElement(StockBean value, Context ctx, Collector<StockBean> out) throws Exception {
                        //日K
                        out.collect(value);
                        //周K
                        ctx.output(weekOpt, value);
                        //月K
                        ctx.output(monthOpt, value);
                    }
                });
        //编写插入SQL 已经存在的就直接替换，没有的就插入数据，始终保持库里的数据是最新的
        String sql = "replace into %s values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
        /**
         * 7.（日、周、月）K线数据写入
         * 数据转换
         * 数据分组
         * 数据写入mysql
         */
        //日K
        processData.map(new StockKlineMap(KlineType.DAYK.getType(), KlineType.DAYK.getFirstTxDateType()))
                /**
                 * The key extractor could return the word as
                 * a key to group all Word objects by the String they contain.
                 */
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(2).toString();
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.stock.sql.day.table"))));

        //周K
        processData.map(new StockKlineMap(KlineType.WEEKK.getType(), KlineType.WEEKK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(2).toString();
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.stock.sql.week.table"))));

        //月K
        processData.map(new StockKlineMap(KlineType.MONTHK.getType(), KlineType.MONTHK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(2).toString();
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.stock.sql.month.table"))));
    }

}
