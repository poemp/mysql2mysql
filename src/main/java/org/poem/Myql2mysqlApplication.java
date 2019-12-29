package org.poem;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @author sangfor
 */
@EnableScheduling
@SpringBootApplication
public class Myql2mysqlApplication {

    public static void main(String[] args) {
        SpringApplication.run(Myql2mysqlApplication.class, args);
    }

}
