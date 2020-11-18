package me.iroohom.service;

import me.iroohom.pojo.User;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * @ClassName: ConsumerService
 * @Author: Roohom
 * @Function: 服务接口
 * @Date: 2020/10/15 21:05
 * @Software: IntelliJ IDEA
 */

@FeignClient(value = "user-service")
public interface ConsumerService {


    @RequestMapping("/user/findById/{id}")
    User findById(@PathVariable("id") Integer id);
}
