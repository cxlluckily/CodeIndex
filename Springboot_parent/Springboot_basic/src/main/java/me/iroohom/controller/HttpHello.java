package me.iroohom.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName: HttpHello
 * @Author: Roohom
 * @Function: Controller处理请求类
 * @Date: 2020/10/13 10:31
 * @Software: IntelliJ IDEA
 */

@RestController
public class HttpHello {


    @GetMapping("/Hello")
    public String Halo()
    {
        return "Hello SpringBoot";
    }
}
