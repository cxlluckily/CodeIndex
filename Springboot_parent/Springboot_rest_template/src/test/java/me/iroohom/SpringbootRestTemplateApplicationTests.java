package me.iroohom;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.RestTemplate;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringbootRestTemplateApplicationTests {

    @Autowired
    private RestTemplate restTemplate;

    @Test
    public void contextLoads() {
//        ResponseEntity<User[]> responseEntity = restTemplate.getForEntity("http:localhosot:8080/user/findAll", User[].class);
//        User[] body = responseEntity.getBody();

        List list = restTemplate.getForObject("http://localhost:8080/user/findAll", List.class);

        System.out.println(list);

    }

}
