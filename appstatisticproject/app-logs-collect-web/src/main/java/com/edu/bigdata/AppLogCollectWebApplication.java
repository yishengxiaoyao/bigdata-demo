package com.edu.bigdata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@SpringBootApplication
@EnableWebMvc
public class AppLogCollectWebApplication {
    public static void main(String[] args){
        SpringApplication.run(AppLogCollectWebApplication.class,args);
    }
}