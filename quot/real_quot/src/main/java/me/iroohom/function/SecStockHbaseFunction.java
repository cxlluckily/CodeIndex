package me.iroohom.function;


import com.alibaba.fastjson.JSON;
import me.iroohom.bean.StockBean;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: SecStockHbaseFunction
 * @Author: Roohom
 * @Function: 秒级数据写入Habse处理函数，封装数据进入List<Put>批量写入
 * @Date: 2020/11/1 20:31
 * @Software: IntelliJ IDEA
 */
public class SecStockHbaseFunction implements AllWindowFunction<StockBean, List<Put>, TimeWindow> {
    /**
     * 开发步骤：
     * 1.新建List<Put>
     * 2.循环数据
     * 3.设置rowkey
     * 4.json数据转换
     * 5.封装put
     * 6.收集数据
     *
     * @param window 窗口
     * @param values 窗口中的数据
     * @param out    输出数据
     * @throws Exception
     */

    @Override
    public void apply(TimeWindow window, Iterable<StockBean> values, Collector<List<Put>> out) throws Exception {
        List<Put> puts = new ArrayList<>();

        for (StockBean value : values) {
            //VITAL: 设置rowkey，rowkey设计规则是证券代码SecCode+交易时间
            String rowkey = value.getSecCode() + value.getTradeTime();
            String string = JSON.toJSONString(value);
            Put put = new Put(rowkey.getBytes());
            //设置列族、列名
            put.add("info".getBytes(), "data".getBytes(), string.getBytes());
            puts.add(put);
        }
        out.collect(puts);
    }
}
