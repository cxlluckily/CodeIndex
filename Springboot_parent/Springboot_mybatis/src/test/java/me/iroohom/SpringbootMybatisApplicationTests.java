package me.iroohom;

import me.iroohom.mapper.UserMapper;
import me.iroohom.pojo.User;
import me.iroohom.service.UserService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringbootMybatisApplicationTests {

    @Autowired
    private UserMapper usermapper;


    @Test
    public void contextLoads() {
        List<User> users = usermapper.queryAll();

        System.out.println("==========================================");
        System.out.println(users);
        System.out.println("==========================================");
    }

    @Test
    public void queryUserById(){
        User user = usermapper.queryUserById(1);
        System.out.println("==========================================");
        System.out.println(user);
        System.out.println("==========================================");
    }

    @Autowired
    private UserService userService;

    @Test
    public void UserServiceTest()
    {
        List<User> all = userService.findAll();
        System.out.println(all );
    }

    @Test
    public void UserService2Test()
    {
        User userById = userService.findUserById(2);
        System.out.println(userById);
    }

}
