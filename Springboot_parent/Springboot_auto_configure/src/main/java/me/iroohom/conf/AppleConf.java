package me.iroohom.conf;

import me.iroohom.pojo.Apple;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * @ClassName: AppleConf
 * @Author: Roohom
 * @Function:
 * @Date: 2020/10/14 16:46
 * @Software: IntelliJ IDEA
 */
@Configuration
public class AppleConf {

    @Bean
    public Apple getApple()
    {
        return new Apple();
    }

}
