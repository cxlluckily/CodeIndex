package me.iroohom.controller;

import me.iroohom.pojo.Account;
import me.iroohom.pojo.QueryUnit;
import org.omg.CORBA.PUBLIC_MEMBER;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @ClassName: AccountController
 * @Author: Roohom
 * @Function: Controller类
 * @Date: 2020/10/13 18:13
 * @Software: IntelliJ IDEA
 */
@RestController
@RequestMapping("/account")
public class AccountController {

    @GetMapping("/show01")
    public String accountShow1(Integer uid, String username, Boolean isVIP) {
        return "收到参数如下:\n" + "uid=" + uid + "\tusername=" + username + "\tisVIP=" + isVIP;
    }

    @GetMapping("/show2")
    public String accountShow2(@RequestParam("uid") Integer id, @RequestParam("username") String name, @RequestParam("isVIP") Boolean flag) {
        return "收到参数如下:\n" + "uid=" + id + "\tusername=" + name + "\tisVIP=" + flag;
    }

    @GetMapping("/show03/{out}/{in}/{money}")
    public String accountShow03(
            @PathVariable("out") String out, @PathVariable("in") String in, @PathVariable("money") Double money
    ) {
        return "收到参数如下:\n" + "out=" + out + "\tin=" + in + "\tmoney=" + money;
    }

    @GetMapping("/show03/{id}.html")
    public String accountShow(@PathVariable("id") Long id)
    {
        return "收到参数:"+id;
    }

    @GetMapping("/show04")
    public String accountShow04(Account account) {
        return "收到参数:" + account;
    }

    @GetMapping("/show05")
    public String accountShow05(QueryUnit queryUnit) {
        return "收到参数" + queryUnit;
    }

    @GetMapping("/show06")
    public QueryUnit accountShow06(QueryUnit queryUnit)
    {
        return queryUnit;
    }

    @GetMapping("/show07")
    public QueryUnit accountShow07(QueryUnit queryUnit)
    {
        return queryUnit;
    }

    @GetMapping("/show08")
    public String accoutShow08(@RequestHeader("User-Agent") String userAgent)
    {
        return userAgent;
    }

}
