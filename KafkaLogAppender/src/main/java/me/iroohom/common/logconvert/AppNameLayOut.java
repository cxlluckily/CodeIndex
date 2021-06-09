package me.iroohom.common.logconvert;

import ch.qos.logback.access.PatternLayout;

/**
 * @ClassName: AppNameLayout
 * @Author: Roohom
 * @Function:
 * @Date: 2021/3/27 22:54
 * @Software: IntelliJ IDEA
 */
public class AppNameLayOut extends PatternLayout {

    static {
        defaultConverterMap.put("yarnJobName", AppNameConvert.class.getName());
    }

}