package me.iroohom.function;

import com.alibaba.fastjson.JSON;
import me.iroohom.bean.SectorBean;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: SectorHbaseListWindowFunction
 * @Author: Roohom
 * @Function: 板块数据写入Hbase窗口处理函数
 * @Date: 2020/11/3 21:42
 * @Software: IntelliJ IDEA
 */
public class SectorHbaseListWindowFunction extends RichAllWindowFunction<SectorBean, List<Put>, TimeWindow> {
    @Override
    public void apply(TimeWindow window, Iterable<SectorBean> values, Collector<List<Put>> out) throws Exception {
        List<Put> list = new ArrayList<>();
        for (SectorBean value : values) {
            //拼接rowkey，封装Put
            Put put = new Put((value.getSectorCode() + value.getTradeTime()).getBytes());
            //添加列族列名数据
            put.add("info".getBytes(), "data".getBytes(), JSON.toJSONString(value).getBytes());
            //将该put封装如List对象
            list.add(put);
        }

        out.collect(list);
    }
}
