package xyz.guqing.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * @author guqing
 * @date 2019-11-27 17:16
 */
@SpringBootApplication
public class KafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaApplication.class, args);
    }

    @GetMapping("/test")
    public String test() {
        return "hello world";
    }
}
