package me.iroohom.inter;

import me.iroohom.bean.CleanBean;
import org.apache.flink.streaming.api.datastream.DataStream;


/**
 * @author roohom
 */
public interface ProcessDataInterface {
    /**
     * 定义接口方法
     * @param waterData 水位线数据
     */
    void  process(DataStream<CleanBean> waterData);
}
