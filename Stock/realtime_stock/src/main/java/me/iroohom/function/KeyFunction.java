package me.iroohom.function;

import me.iroohom.bean.CleanBean;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * @ClassName: KeyFunction
 * @Author: Roohom
 * @Function:
 * @Date: 2020/11/15 21:35
 * @Software: IntelliJ IDEA
 */
public class KeyFunction implements KeySelector<CleanBean,String> {
    @Override
    public String getKey(CleanBean value) throws Exception {
        return value.getSecCode();
    }
}
