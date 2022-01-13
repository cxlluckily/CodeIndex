package com.svw.bdp;

import com.svw.bdp.common.conf.Config;
import com.svw.bdp.common.utils.DateUtil;
import com.svw.bdp.processor.StreamHandle;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Properties;

public class Application {
    /**
     * 启动环境（local、test、prod，默认local）
     */
    private static String env = "local";

    /**
     * 日期（默认昨天）
     */
    private static String dt = DateUtil.millis2String(DateUtil.getLastOrNextNDay(System.currentTimeMillis(), -1), "yyyy-MM-dd");

    /**
     * 加载参数
     *
     * @param args 参数数组
     */
    private static void loadArgs(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("c", "conf", true, "set configure file (Re: local, test, prod)");
        options.addOption("d", "dt", true, "set dt");

        CommandLine cmd = new DefaultParser().parse(options, args);
        if (cmd.hasOption("c")) {
            env = cmd.getOptionValue("c");
        }
        if (cmd.hasOption("d")) {
            dt = cmd.getOptionValue("d");
        }
    }

    public static void main(String[] args) throws Exception {
        //解析参数
        loadArgs(args);

        //加载配置
        Properties properties = Config.getProperties(env);

        //handle
        new StreamHandle().doIt(properties);
    }
}
