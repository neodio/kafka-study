package com.kafkastudy.kafkastudy;

import com.google.gson.Gson;
import com.kafkastudy.kafkastudy.api.SampleSendDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.HashMap;
import java.util.Map;

@DisplayName("기본 producer/consumer 테스트")
@Slf4j
@SpringBootTest
public class BasicProducerConsumerTest {

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @BeforeEach
    void init() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        ContainerProperties containerProps = new ContainerProperties("test0923");
        containerProps.setGroupId("test-group1");
        containerProps.setMessageListener((MessageListener<String, String>) data ->
                log.info("onMessage topic : {}, key:{}, value:{} ", data.topic(), data.key(), data.value()));

        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(configs);
        KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        container.start();
    }

    @DisplayName("producer 테스트")
    @Test
    void testProducer() {
        // given
        SampleSendDto sampleSendDto = SampleSendDto.builder().prodNo(1L).prodNm("7").build();

        // when,then
        Assertions.assertDoesNotThrow(() -> kafkaTemplate.send("test0923", "testKey", sampleSendDto)
                .addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
                    @Override
                    public void onFailure(Throwable ex) {
                        ex.printStackTrace();
                    }

                    @Override
                    public void onSuccess(SendResult<String, Object> result) {
                        log.info("send onSuccess : {}", new Gson().toJson(result.getRecordMetadata()));
                    }
                }));

    }
}
