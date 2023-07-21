package com.kafkastudy.kafkastudy.consumer;

import com.kafkastudy.kafkastudy.constants.CodeConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class BatchModeConsumer {

    @KafkaListener(groupId = "batch-group", id = "batch-consumer", topics = CodeConstants.BATCH_TOPIC_NAME, containerFactory = "batchFactory")
    public void onMessage(List<ConsumerRecord<String, String>> data) {
        log.info("batch receive Data size : {}", data.size());
        data.forEach(record -> log.info("batch receive Data key:{} ,data:{}", record.key(), record.value()));
    }

}
