package com.kafkastudy.kafkastudy.consumer;

import com.google.gson.Gson;
import com.kafkastudy.kafkastudy.api.SampleSendDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("auto.offset.reset 옵션 테스트")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Slf4j
@SpringBootTest
public class AutoOffsetResetTest {

    @Autowired
    AdminClient adminClient;

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TOPIC_NAME = "test-autoOffsetReset";
    private static final String GROUP_NAME = "autoOffsetReset-group";
    private static final String NEW_GROUP_NAME = GROUP_NAME + "-1";

    @DisplayName("테스트 토픽 생성")
    @Order(value = 1)
    @Test
    void createTopic() throws ExecutionException, InterruptedException {
        // given
        NewTopic newTopic = TopicBuilder.name(TOPIC_NAME).partitions(2).replicas(1).build();

        // when
        KafkaFuture<Void> kafkaFuture = adminClient.createTopics(Collections.singletonList(newTopic)).all();
        kafkaFuture.get();

        // then
        assertFalse(kafkaFuture.isCompletedExceptionally());
        assertTrue(kafkaFuture.isDone());
    }

    private KafkaMessageListenerContainer<String, String> getKafkaMessageListenerContainer(String groupName,
                                                                                           String autoOffsetReset,
                                                                                           MessageListener<String, String> messageListener) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        ContainerProperties containerProps = new ContainerProperties(TOPIC_NAME);
        containerProps.setGroupId(groupName);
        containerProps.setMessageListener(messageListener);

        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(configs);
        return new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
    }

    @DisplayName("데이터 추가에 따른 consumer 테스트")
    @Order(value = 2)
    @Test
    void testBasicProducerAndConsumer() {
        // given
        MessageListener<String, String> messageListener = data ->
                log.info("> basic Consumer onMessage > topic : {}, key:{}, value:{} , offset:{}", data.topic(), data.key(), data.value(), data.offset());
        getKafkaMessageListenerContainer(GROUP_NAME, "latest", messageListener).start();

        LongStream.rangeClosed(1, 5).forEach(prodNo -> {
            SampleSendDto sampleSendDto = SampleSendDto.builder().prodNo(prodNo).prodNm("testProdNo " + prodNo).build();

            // when,then
            Assertions.assertDoesNotThrow(() -> kafkaTemplate.send(TOPIC_NAME, "testKey", sampleSendDto)
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
        });
    }

    @DisplayName("신규 컨슈머 추가시, AUTO_OFFSET_RESET_CONFIG 옵션을 통해 처음 부터 데이터 조회 테스트")
    @Order(value = 3)
    @Test
    void testAutoOffsetReset() throws InterruptedException {
        // given
        MessageListener<String, String> messageListener = data ->
                log.info("> new Consumer onMessage > topic : {}, key:{}, value:{} , offset:{}", data.topic(), data.key(), data.value(), data.offset());

        // when, then
        Assertions.assertDoesNotThrow(() -> getKafkaMessageListenerContainer(NEW_GROUP_NAME, "earliest", messageListener).start());

        Thread.sleep(2000);
    }

    @DisplayName("테스트 토픽 삭제")
    @Order(value = 4)
    @Test
    void deleteTopic() throws ExecutionException, InterruptedException {
        adminClient.deleteTopics(Collections.singletonList(TOPIC_NAME)).all().get();
    }
}
