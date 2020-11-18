package me.iroohom.function;

import me.iroohom.bean.CleanBean;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * @ClassName: KeyFunction
 * @Author: Roohom
 * @Function: 分组函数，根据SecCode代码对数据进行分组
 * @Date: 2020/11/1 09:47
 * @Software: IntelliJ IDEA
 */
public class KeyFunction implements KeySelector<CleanBean,String> {
    @Override
    public String getKey(CleanBean value) throws Exception {
        return value.getSecCode();
    }
}
