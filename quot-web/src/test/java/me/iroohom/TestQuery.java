package me.iroohom;

import me.iroohom.mapper.QuotMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.Map;

/**
 * @ClassName: TestRedis
 * @Author: Roohom
 * @Function: 测试redis连接
 * @Date: 2020/11/9 19:12
 * @Software: IntelliJ IDEA
 */

@SpringBootTest
@RunWith(SpringRunner.class)
public class TestQuery {
    @Autowired
    QuotMapper quotMapper;

    @Test
    public void mybatisQuery() {
        List<Map<String, Object>> query = quotMapper.query();
        System.out.println("测试结果:---->" + query);
    }

    @Autowired
    JedisCluster jedisCluster;

    @Test
    public void redisQuery() {
        String hget = jedisCluster.hget("quot", "turnoverRate");
        System.out.println(hget);
    }
}
