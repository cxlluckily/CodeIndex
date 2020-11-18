package me.iroohom.controller;

import me.iroohom.pojo.Books;
import me.iroohom.pojo.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;


/**
 * @ClassName: HelloController
 * @Author: Roohom
 * @Function: Controller类
 * @Date: 2020/10/13 11:29
 * @Software: IntelliJ IDEA
 */


@RestController
public class HelloController {

    @Value("${name}")
    private String name;

    @Value("${date}")
    private String date;

    //自动注入
    @Autowired
    private Person person;

    @Autowired
    private Books books;

//    @Autowired
//    private Books books;


    @GetMapping("/hello")
    public String hello() {
        return "hello Spingboot model02" + "\n" + name + "\t" + date + "\t" + person;
    }

    @GetMapping("/books")
    public String books()
    {
        return "books are:"+books;
    }

}
