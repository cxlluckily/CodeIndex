package me.iroohom.util;

import me.iroohom.config.QuotConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;

/**
 * @ClassName: RedisUtil
 * @Author: Roohom
 * @Function: Redis工具类
 * @Date: 2020/10/31 21:23
 * @Software: IntelliJ IDEA
 */
public class RedisUtil {

    public static JedisCluster getJedisCluster() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(Integer.parseInt(QuotConfig.config.getProperty("redis.maxTotal")));
        jedisPoolConfig.setMaxIdle(Integer.parseInt(QuotConfig.config.getProperty("redis.maxIdle")));
        jedisPoolConfig.setMinIdle(Integer.parseInt(QuotConfig.config.getProperty("redis.minIdle")));

        //设置连接Redis集群地址
        HashSet<HostAndPort> hostAndPorts = new HashSet<>();
        String host = QuotConfig.config.getProperty("redis.host");
        String[] split = host.split(",");
        for (String ht : split) {
            String[] split1 = ht.split(":");
            hostAndPorts.add(new HostAndPort(split1[0], Integer.parseInt(split1[1])));
        }

        return new JedisCluster(hostAndPorts, jedisPoolConfig);
    }


    public static void main(String[] args) {

        //获取连接对象
        JedisCluster jedisCluster = getJedisCluster();
//        String hget = jedisCluster.hget("product", "apple");
//        System.out.println(hget);

//        jedisCluster.hset("product", "apple", "10");
//        jedisCluster.hset("product", "rice", "6");
//        jedisCluster.hset("product", "flour", "6");
//        jedisCluster.hset("product", "banana", "8");
//        jedisCluster.hset("product", "mask", "5");

        //添加预警阀值
        jedisCluster.hset("quot", "amplitude", "-1");//振幅
        jedisCluster.hset("quot", "upDown1", "-1"); //涨跌幅-跌幅
        jedisCluster.hset("quot", "upDown2", "100");//涨跌幅-涨幅
        jedisCluster.hset("quot", "turnoverRate", "-1");//换手率


    }

}
