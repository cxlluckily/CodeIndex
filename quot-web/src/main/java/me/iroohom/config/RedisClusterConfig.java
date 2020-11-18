package me.iroohom.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;

/**
 * @ClassName: RedisClusterConfig
 * @Author: Roohom
 * @Function: 获取Redis连接对象
 * @Date: 2020/11/9 09:50
 * @Software: IntelliJ IDEA
 */



/**
 * 表示配置文件，包含bean对象，项目启动，会优先加载配置文件，会把bean对象置于Spring容器进行管理
 * @author roohom
 */
@Configuration
public class RedisClusterConfig {
    @Value("${redis.maxtotal}")
    private int maxtotal;

    @Value("${redis.minIdle}")
    private int minIdle;

    @Value("${redis.maxIdle}")
    private int maxIdle;

    @Value("${redis.address}")
    private String address;


    @Bean
    public JedisCluster getJedis() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(maxtotal);
        jedisPoolConfig.setMinIdle(minIdle);
        jedisPoolConfig.setMaxIdle(maxIdle);

        String[] split = address.split(",");
        HashSet<HostAndPort> hostAndPorts = new HashSet<>();
        for (String hostAndPort : split) {
            String[] hp = hostAndPort.split(":");
            String host = hp[0];
            int port = Integer.parseInt(hp[1]);
            hostAndPorts.add(new HostAndPort(host, port));

        }
        return new JedisCluster(hostAndPorts, jedisPoolConfig);
    }
}
