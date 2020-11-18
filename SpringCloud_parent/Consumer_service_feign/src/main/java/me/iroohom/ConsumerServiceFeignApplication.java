package me.iroohom;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.SpringCloudApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
@EnableDiscoveryClient
//@EnableCircuitBreaker
//开启Feign
@EnableFeignClients
public class ConsumerServiceFeignApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConsumerServiceFeignApplication.class, args);
    }


    /**
     * 注入restTemplate
     *
     * @return RestTemplate对象
     */
    //开启负载均衡
    @LoadBalanced
    @Bean
    public RestTemplate createRestTemplate() {
        return new RestTemplate();
    }

}
