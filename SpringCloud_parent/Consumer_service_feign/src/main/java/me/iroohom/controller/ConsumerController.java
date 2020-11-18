package me.iroohom.controller;

import me.iroohom.pojo.User;
import me.iroohom.service.ConsumerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.List;

/**
 * @ClassName: ConsumerController
 * @Author: Roohom
 * @Function: Controller控制器处理网页请求
 * @Date: 2020/10/15 14:52
 * @Software: IntelliJ IDEA
 */

@RestController
@RequestMapping("consumer")
public class ConsumerController {


    @Autowired
    private ConsumerService consumerService;


    @Value("${server.port}")
    private Integer port;


    @GetMapping("/findById/{id}")
    public User findById(@PathVariable("id") int id) {
        User user = consumerService.findById(id);
        user.setNote(user.getNote() + ",服务消费者的端口号是:" + port);
        return user;
    }

}














