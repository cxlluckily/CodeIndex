package me.iroohom.conf;

import me.iroohom.pojo.Apple;
import me.iroohom.pojo.User;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * @ClassName: SpringConf
 * @Author: Roohom
 * @Function: 配置类
 * @Date: 2020/10/14 16:36
 * @Software: IntelliJ IDEA
 */
@Configuration
public class SpringConf {

    @Bean
    //@Conditional(MyCondition.class)
    //如果当前在容器中没有apple对象，那么就构建当前对象并放置到容器中
//    @ConditionalOnMissingBean(Apple.class)
    //如果当前在容器中有apple对象，那么就构建当前对象并放置到容器中
//    @ConditionalOnBean(Apple.class)
    //和配置文件中的内容对比，如果一直，就构建当前对象并放置到容器中
    @ConditionalOnProperty(name = "name",havingValue = "springboot")
    public User getUser() {
        return new User("张三", "123456");
    }
}
