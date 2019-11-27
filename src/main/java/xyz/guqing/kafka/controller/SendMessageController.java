package xyz.guqing.kafka.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.NonNull;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * <p> kafka消息发送controller </p>
 *
 * @author guqing
 * @date 2019-11-27 19:50
 */
@RestController
public class SendMessageController {
    private static final Logger logger = LoggerFactory.getLogger(SendMessageController.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public SendMessageController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

//    @GetMapping("send/{message}")
//    public void send(@PathVariable String message) {
//        // 向test_topic中发送消息
//        // send方法是一个异步方法，所以通过监听器来判断消息是否发送成功
//        this.kafkaTemplate.send("test_topic", message);
//    }

    @GetMapping("send/{message}")
    public void send(@PathVariable String message) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("test_topic", message);
        // 回调
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>(){

            @Override
            public void onSuccess(SendResult<String, String> result) {
                logger.info("成功发送消息：{}，offset=[{}]", message, result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(@NonNull Throwable throwable) {
                logger.info("消息:{},发送失败，原因={}", message, throwable.getMessage());
            }
        });
    }
}
