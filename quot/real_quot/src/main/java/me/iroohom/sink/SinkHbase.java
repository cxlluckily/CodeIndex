package me.iroohom.sink;

import me.iroohom.util.HbaseUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.client.Put;

import java.util.List;

/**
 * @ClassName: SinkHbase
 * @Author: Roohom
 * @Function: Sink函数，数据写入Hbase实际处理逻辑
 * @Date: 2020/11/1 20:47
 * @Software: IntelliJ IDEA
 */
public class SinkHbase implements SinkFunction<List<Put>> {

    private String tableName;

    public SinkHbase(String tableName) {
        this.tableName = tableName;
    }

    /**
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<Put> value, Context context) throws Exception {
        HbaseUtil.putList(tableName,value);
    }
}
