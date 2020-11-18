package me.iroohom.controller;

import me.iroohom.pojo.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.util.List;

/**
 * @ClassName: OrderSystemController
 * @Author: Roohom
 * @Function: controller
 * @Date: 2020/10/14 11:46
 * @Software: IntelliJ IDEA
 */
@RestController
@RequestMapping("/order")
public class OrderSystemController {
    @Autowired
    private RestTemplate restTemplate;


    @GetMapping("/findStore")
    public User queryUserById(Integer id)
    {
        User user = restTemplate.getForObject("http://localhost:8080/user/findUserById?id=" + id, User.class);
        return user;
    }

}
