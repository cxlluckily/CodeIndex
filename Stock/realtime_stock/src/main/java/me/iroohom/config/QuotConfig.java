package me.iroohom.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @ClassName: QuotConfig
 * @Author: Roohom
 * @Function: 配置类，用于读取配置文件
 * @Date: 2020/10/31 10:14
 * @Software: IntelliJ IDEA
 */
public class QuotConfig {
    public static Properties config = new Properties();

    static {
        InputStream inputStream = QuotConfig.class.getClassLoader().getResourceAsStream("config.properties");
        try {
            config.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println(config.getProperty("close.time"));
    }


}
