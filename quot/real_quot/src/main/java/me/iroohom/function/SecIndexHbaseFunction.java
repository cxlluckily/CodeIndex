package me.iroohom.function;

import com.alibaba.fastjson.JSON;
import me.iroohom.bean.IndexBean;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: SecIndexHbaseFunction
 * @Author: Roohom
 * @Function: 秒级指数写入Hbase
 * @Date: 2020/11/5 13:11
 * @Software: IntelliJ IDEA
 */
public class SecIndexHbaseFunction extends RichAllWindowFunction<IndexBean, List<Put>, TimeWindow> {
    @Override
    public void apply(TimeWindow window, Iterable<IndexBean> values, Collector<List<Put>> out) throws Exception {
        ArrayList<Put> puts = new ArrayList<>();
        for (IndexBean value : values) {
            //拼接rowkey
            String rowkey = value.getIndexCode() + value.getTradeTime();
            String jsonString = JSON.toJSONString(value);
            Put put = new Put(rowkey.getBytes());
            put.add("info".getBytes(), "data".getBytes(), jsonString.getBytes());
            puts.add(put);
        }

        //收集数据
        out.collect(puts);
    }
}
