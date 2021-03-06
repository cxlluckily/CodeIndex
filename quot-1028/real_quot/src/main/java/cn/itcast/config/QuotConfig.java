package me.iroohom.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Date 2020/10/31
 */
public class QuotConfig {

    //加载配置文件
    public static Properties config = new Properties();
    static {

        //类加载器获取resources下的配置文件
        InputStream in = QuotConfig.class.getClassLoader().getResourceAsStream("config.properties");
        try {
            config.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //打印测试
    public static void main(String[] args) {
        System.out.println(config.getProperty("close.time"));
    }
}
