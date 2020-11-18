package me.iroohom.pojo;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * @ClassName: Books
 * @Author: Roohom
 * @Function:
 * @Date: 2020/10/13 17:49
 * @Software: IntelliJ IDEA
 */

@ConfigurationProperties(prefix = "books")
@Component
@Data
public class Books {

    private List<Book> books;

}
