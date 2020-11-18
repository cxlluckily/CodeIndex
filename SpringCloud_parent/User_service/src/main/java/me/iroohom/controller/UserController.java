package me.iroohom.controller;

import me.iroohom.pojo.User;
import me.iroohom.service.UserService;
import org.apache.ibatis.annotations.Param;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * 用户信息表(User)表控制层
 *
 * @author roohom
 * @since 2020-10-15 14:27:43
 */
@RestController
@RequestMapping("user")
public class UserController {
    /**
     * 服务对象
     */
    @Resource
    private UserService userService;


    @Value("${server.port}")
    private int port;

    /**
     * 通过主键查询单条数据
     *
     * @param id 主键
     * @return 单条数据
     */
    @GetMapping("/findById/{id}")
    public User findById(@PathVariable("id") Integer id) {
//        return this.userService.queryById(id);

        User user = this.userService.queryById(id);

        // 目标: 为了看到 负载均衡 的效果
        // 将端口放到user对象的 备注中
        user.setNote("服务提供者的端口号: " + port);

        return user;
    }
}