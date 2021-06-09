package me.iroohom.boot.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName: MyController
 * @Author: Roohom
 * @Function:
 * @Date: 2021/5/22 18:22
 * @Software: IntelliJ IDEA
 */
@RestController
public class MyController {

    @RequestMapping("/hello")
    public String handle() {
        return "Hello, SpringBoot!";
    }
}
