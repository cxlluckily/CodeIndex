package me.roohom.common;

import java.io.IOException;
import java.util.Properties;

/**
 * 加载配置文件
 */
public class Config {

    private static final String SUFFIX = ".properties";

    /**
     * 加载配置
     *
     * @param env 启动环境
     * @return Properties类
     * @throws IOException 目标文件找不到
     */
    public static Properties getProperties(String env) throws IOException {
        Properties properties = new Properties();
        String proName = (env == null) ? "local" : env;
        properties.load(Config.class.getClassLoader().getResourceAsStream(proName + SUFFIX));
        return properties;
    }
}
