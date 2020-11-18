package me.iroohom.controller;

import me.iroohom.mapper.UserMapper;
import me.iroohom.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName: UserController
 * @Author: Roohom
 * @Function: Controller类 接收网页处理请求
 * @Date: 2020/10/14 19:03
 * @Software: IntelliJ IDEA
 */
@RestController
@RequestMapping("/account")
public class UserController {


    @Autowired
    private UserService userService;

    @Autowired
    private UserMapper userMapper;

    @GetMapping("/pay")
    public String transerMoney(String outName, String inName, Double money){
        try {
            userService.transfer(outName, inName, money);
        }
        catch (Exception  e)
        {
            return e.toString();
        }
        return "哦豁！你完成了一次专账！";
    }


}
