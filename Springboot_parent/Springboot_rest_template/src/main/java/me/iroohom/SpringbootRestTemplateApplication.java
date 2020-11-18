package me.iroohom;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
public class SpringbootRestTemplateApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringbootRestTemplateApplication.class, args);
    }


    @Bean
    public RestTemplate getRestTesmplate() {
        return new RestTemplate();
    }

}
