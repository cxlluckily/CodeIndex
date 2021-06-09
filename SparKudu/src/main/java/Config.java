import java.io.IOException;
import java.util.Properties;

/**
 * @ClassName: Config
 * @Author: Roohom
 * @Function:
 * @Date: 2021/4/29 17:35
 * @Software: IntelliJ IDEA
 */
public class Config {

    public static String SUFFIX = ".properties";


    public static Properties getProperties() throws IOException {
        Properties properties = new Properties();
        properties.load(Config.class.getClassLoader().getResourceAsStream("local" + SUFFIX));

        return properties;
    }
}
