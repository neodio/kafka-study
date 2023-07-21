package com.kafkastudy.kafkastudy.topic;

import com.google.gson.Gson;
import com.kafkastudy.kafkastudy.api.SampleSendDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.LogIfLevelEnabled;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@SpringBootTest
public class RetentionMsTest {
    @Autowired
    AdminClient adminClient;

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private static final String TOPIC_NAME = "test-cleanup-policy-topic";

    @DisplayName("데이터 3초 동안 저장하는 테스트 토픽 생성")
    @Test
    void testCreateTopic() {
        // given
        NewTopic newTopic = TopicBuilder.name(TOPIC_NAME)
                .partitions(2)
                .replicas(2)
                .config(TopicConfig.RETENTION_MS_CONFIG, "3000")
                .build();

        // when,then
        Assertions.assertDoesNotThrow(() -> adminClient.createTopics(Collections.singletonList(newTopic)).all().get());
    }

    @DisplayName("테스트 토픽 삭제")
    @Test
    void deleteTopic() {
        // given
        List<String> topics = Collections.singletonList(TOPIC_NAME);

        // when,then
        Assertions.assertDoesNotThrow(() -> adminClient.deleteTopics(topics).all().get());
    }

    /**
     * 테스트 진행시, docker-compose 파일 KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS 주석 해제 및 재시작 필요
     * LOG_RETENTION_CHECK_INTERVAL_MS (df:5min) - 삭제할 대상 확인 주기
     */
    @DisplayName("토픽 데이터 보존 시간에 따른 데이터 삭제 테스트")
    @Test
    void testRetentionMs() throws InterruptedException {
        // given
        AtomicInteger receivedCount = new AtomicInteger();
        long prodNo = 1L;

        SampleSendDto sampleSendDto = SampleSendDto.builder().prodNo(prodNo).prodNm(TOPIC_NAME + " testProdNo " + prodNo).build();

        // when
        kafkaTemplate.send(TOPIC_NAME, String.valueOf(prodNo), sampleSendDto).addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error(ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {
                log.info("[{}] send onSuccess : {}", Thread.currentThread().getName(), new Gson().toJson(result.getRecordMetadata()));
            }
        });


        Thread.sleep(6000);

        AcknowledgingMessageListener<String, String> messageListener = (data, acknowledgment) -> {
            receivedCount.getAndIncrement();
            log.info("> Consumer onMessage > topic : {}, key:{}, value:{} , offset:{}", data.topic(), data.key(), data.value(), data.offset());
        };

        ContainerProperties containerProps = new ContainerProperties(TOPIC_NAME);
        containerProps.setGroupId(TOPIC_NAME + "-group");
        containerProps.setAckMode(ContainerProperties.AckMode.MANUAL);
        containerProps.setMessageListener(messageListener);
        containerProps.setCommitLogLevel(LogIfLevelEnabled.Level.DEBUG);
        containerProps.setLogContainerConfig(true);
        containerProps.setSyncCommits(false);
        containerProps.setCommitCallback((offsets, exception) -> offsets.forEach((key, value) -> {
            log.info(">>>> onComplete topic:{}, partition:{}, offset:{}, leaderEpoch:{}", key.topic(), key.partition(), value.offset(), value.leaderEpoch());
        }));

        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(configs);
        KafkaMessageListenerContainer<String, String> kafkaMessageListenerContainer = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        kafkaMessageListenerContainer.start();

        Thread.sleep(1000);

        // then
        assertEquals(0, receivedCount.get());
    }
}
