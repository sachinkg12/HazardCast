package com.hazardcast;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class HazardCastApplication {

    public static void main(String[] args) {
        SpringApplication.run(HazardCastApplication.class, args);
    }
}
