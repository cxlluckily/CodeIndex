package me.iroohom.pojo;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @ClassName: person
 * @Author: Roohom
 * @Function: java bean
 * @Date: 2020/10/13 11:49
 * @Software: IntelliJ IDEA
 */

// @ConfigurationProperties(prefix = "person") 将配置文件信息注入到这个pojo类中
@ConfigurationProperties(prefix = "person")
// 将当前类 放到spring容器中
@Component
@Data
public class Person {

    private String username;
    private String password;
    private String sex;
    private String address;

    private String[] hobbies;
    private List<Book> books;

}
