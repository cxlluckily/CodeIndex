package me.roohom.mbb;

import me.roohom.common.Config;
import me.roohom.mbb.job.BatchFeedbackJob;
import me.roohom.mbb.job.BatchSubscribeJob;
import me.roohom.mbb.job.MbbFetchDataJob;
import me.roohom.utils.DateUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.util.Properties;

/**
 * cdc snapshot
 *
 * @author WY
 */
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
     * 执行的任务
     */
    private static String part = "default";

    /**
     * 加载参数
     *
     * @param args 参数数组
     */
    private static Properties loadArgs(String[] args) throws ParseException, IOException {
        //解析配置
        Properties properties = Config.getProperties(env);

        Options options = new Options();
        options.addOption("c", "conf", true, "set configure file (Re: local, test, prod)");
        options.addOption("d", "dt", true, "set dt");
        options.addOption("p", "part", true, "choose which part to execute(Re: p1, p2, p3)");

        CommandLine cmd = new DefaultParser().parse(options, args);
        if (cmd.hasOption("c")) {
            env = cmd.getOptionValue("c");
        }
        if (cmd.hasOption("d")) {
            dt = cmd.getOptionValue("d");
        }
        if (cmd.hasOption("p")) {
            part = cmd.getOptionValue("p");
        }

        return properties;
    }

    public static void main(String[] args) throws Exception {
        //加载参数、配置
        Properties properties = loadArgs(args);

        switch (part) {
            case "p1":
                new BatchSubscribeJob().handle(properties);
            case "p2":
                new BatchFeedbackJob().handle(properties);
            case "p3":
                new MbbFetchDataJob().handle(properties);
        }


    }
}
