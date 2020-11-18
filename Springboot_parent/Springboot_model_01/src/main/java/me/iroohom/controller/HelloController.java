package me.iroohom.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName: HelloController
 * @Author: Roohom
 * @Function:
 * @Date: 2020/10/13 11:05
 * @Software: IntelliJ IDEA
 */
@RestController
public class HelloController {

    @GetMapping("/Hello")
    public String Halo()
    {
        return "Hello SpringBoot Model!";
    }

}
