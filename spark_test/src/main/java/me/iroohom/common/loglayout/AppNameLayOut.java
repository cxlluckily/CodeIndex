package me.iroohom.common.loglayout;

/**
 * @ClassName: AppNameLayout
 * @Author: Roohom
 * @Function:
 * @Date: 2021/3/23 22:36
 * @Software: IntelliJ IDEA
 */

import ch.qos.logback.access.PatternLayout;
import me.iroohom.common.logconvert.AppNameConvert;

/**
 * 自定义应用名称layout
 * @author Eights
 */
public class AppNameLayOut extends PatternLayout {

    static {
        defaultConverterMap.put("app", AppNameConvert.class.getName());
    }

}