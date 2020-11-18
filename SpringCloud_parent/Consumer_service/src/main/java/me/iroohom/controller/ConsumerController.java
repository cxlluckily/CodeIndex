package me.iroohom.controller;

import com.netflix.hystrix.contrib.javanica.annotation.HystrixCommand;
import me.iroohom.pojo.User;
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
    private RestTemplate restTemplate;

    @Autowired
    private DiscoveryClient discoveryClient;


//    @GetMapping("/findById/{id}")
//    public User findById(@PathVariable("id") Integer id) {
//        return restTemplate.getForObject("http://localhost:8080/user/findById/" + id, User.class);
//    }


    @GetMapping("/findById/{id}")
    @HystrixCommand(fallbackMethod = "fallbackMethod")
    public User findById(@PathVariable("id") int id) {
//        // 1 获取Eureka中注册的 user-service 实例列表
//        List<ServiceInstance> instanceList = discoveryClient.getInstances("user-service");
//
//        // 2 获取实例
//        ServiceInstance serviceInstance = instanceList.get(0);
//
//        // 3 通过实例的信息拼接的请求地址
//        String url = serviceInstance.getUri() + "/user/findById/" + id;
//        System.out.println("===== 方式二 url : " + url);
//        return restTemplate.getForObject(url, User.class);
//
        /**
         * 获取服务提供者的ip地址和端口号 方式三:
         * 不再手动获取ip和端口，而是直接通过服务名称调用
         */
        String url = "http://user-service/user/findById/" + id;
        return restTemplate.getForObject(url, User.class);

    }

    //熔断方法
    public User fallbackMethod(int id)
    {
        User user = new User();
        user.setId(id);
        user.setNote("熔断方法 ... ...");
        return user;
    }





    /**
     * 获取元数据
     *
     * @return 元数据实例集合
     */
    @GetMapping("/getInstances")
    public List<ServiceInstance> getInstances() {

        //serviceId是在eureka里面注册成功的服务名，User_service|user_service都不对
        return discoveryClient.getInstances("user-service");
    }

}
