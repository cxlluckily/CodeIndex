package me.iroohom.controller;

import me.iroohom.pojo.User;
import me.iroohom.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @ClassName: UserController
 * @Author: Roohom
 * @Function: UserController
 * @Date: 2020/10/14 10:20
 * @Software: IntelliJ IDEA
 */
@RestController
@RequestMapping("/user")
public class UserController {

    @Autowired
    private UserService userService;

    @GetMapping("/findAll")
    public List<User> findAll() {
        return userService.findAll();
    }

    @GetMapping("/findUserById")
    public User findUserById(Integer id) {
        return userService.findUserById(id);
    }

}
