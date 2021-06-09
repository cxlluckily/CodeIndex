package me.iroohom.common.logconvert;

/**
 * @ClassName: AppNameLayout
 * @Author: Roohom
 * @Function:
 * @Date: 2021/3/23 22:29
 * @Software: IntelliJ IDEA
 */

import ch.qos.logback.classic.pattern.ClassicConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import org.apache.flink.util.StringUtils;

/**
 * 获取flink应用的java环境变量传递的应用名称并添加到日志中
 *
 * @author Eights
 */
public class AppNameConvert extends ClassicConverter {

    private static final String JOB_NAME = "job.name";

    private static String appName = "应用默认名称";

    //应用名称,这里就可以获取yarn application id, 运行机器之类的指标打到日志上
    static {
        String jobName = System.getProperty(JOB_NAME);
        if (!StringUtils.isNullOrWhitespaceOnly(jobName)) {
            appName = jobName;
        }
    }

    @Override
    public String convert(ILoggingEvent iLoggingEvent) {
        return appName;
    }


}

