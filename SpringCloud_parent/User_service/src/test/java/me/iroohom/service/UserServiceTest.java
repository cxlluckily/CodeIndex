package me.iroohom.service;

import me.iroohom.pojo.User;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;


@RunWith(SpringRunner.class)
@SpringBootTest
public class UserServiceTest {
    @Autowired
    private UserService userService;

    @Test
    public void queryById() {
        User user = userService.queryById(1);
        System.out.println(user);
    }

    @Test
    public void queryAllByLimit() {
        List<User> users = userService.queryAllByLimit(0,3);
        System.out.println(users);
    }

    @Test
    public void insert() {
    }

    @Test
    public void update() {
    }

    @Test
    public void deleteById() {
    }
}