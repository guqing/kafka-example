package xyz.guqing.kafka.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * <p> kafka消息监听器，监听消息的收发 </p>
 *
 * @author guqing
 * @date 2019-11-27 20:13
 */
@Component
public class KafkaMessageListener {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageListener.class);

    @KafkaListener(topics = "test_topic", groupId = "test-consumer")
    public void listen(String message) {
        logger.info("接收到消息: {}", message);
    }
}
