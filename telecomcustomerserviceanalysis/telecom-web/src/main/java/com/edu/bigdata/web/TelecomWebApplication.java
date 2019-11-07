package com.edu.bigdata.web;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@SpringBootApplication
public class TelecomWebApplication {
    public static void main(String[] args){
        SpringApplication.run(TelecomWebApplication.class,args);
    }
}