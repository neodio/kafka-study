package com.kafkastudy.kafkastudy.consumer;

import com.kafkastudy.kafkastudy.constants.CodeConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class SampleConsumer {

    @KafkaListener(id = "testGroup", topics = CodeConstants.SAMPLE_TOPIC_NAME, autoStartup = "false")
    public void listen(String data) {
        log.info("receive data : {}", data);
    }
}
