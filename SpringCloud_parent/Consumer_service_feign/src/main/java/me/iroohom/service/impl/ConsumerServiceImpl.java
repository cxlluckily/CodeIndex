package me.iroohom.service.impl;

import me.iroohom.pojo.User;
import me.iroohom.service.ConsumerService;
import org.springframework.stereotype.Service;


/**
 * @ClassName: ConsumerServiceImpl
 * @Author: Roohom
 * @Function: ConsumerService实现类
 * @Date: 2020/10/15 21:15
 * @Software: IntelliJ IDEA
 */

@Service
public class ConsumerServiceImpl implements ConsumerService {
    @Override
    public User findById(Integer id) {
        User user = new User();
        user.setId(id);
        user.setNote("Feign中的熔断方法......");
        return user;
    }
}
