package com.kafkastudy.kafkastudy.consumer;

import com.kafkastudy.kafkastudy.constants.CodeConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ManualModeConsumer implements AcknowledgingMessageListener<String, String> {

    @Override
    @KafkaListener(groupId = "manual-group", id = "manual-consumer", topics = CodeConstants.MANUAL_TOPIC_NAME, containerFactory = "manualKafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> data, Acknowledgment acknowledgment) {
        log.info("manual receive Data key:{} ,data:{}", data.key(), data.value());
        acknowledgment.acknowledge();
    }
}
