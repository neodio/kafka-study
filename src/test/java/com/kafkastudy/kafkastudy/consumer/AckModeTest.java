package com.kafkastudy.kafkastudy.consumer;

import com.google.gson.Gson;
import com.kafkastudy.kafkastudy.api.SampleSendDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.LogIfLevelEnabled;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@SpringBootTest
public class AckModeTest {
    @Autowired
    AdminClient adminClient;

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    private static final String MANUAL_TOPIC_NAME = "test-ackMode-manual";
    private static final String MANUAL_GROUP_NAME = "ackMode-group-manual";
    private static final String MANUAL_IMMEDIATE_TOPIC_NAME = "test-ackMode-manual-immediate";
    private static final String MANUAL_IMMEDIATE_GROUP_NAME = "ackMode-group-manual-immediate";

    private static final String BATCH_TOPIC_NAME = "test-batch";
    private static final String BATCH_GROUP_NAME = "test-ackMode-batch";
    private static final String COUNT_TOPIC_NAME = "test-count";
    private static final String COUNT_GROUP_NAME = "test-ackMode-count";

    @DisplayName("테스트 토픽 생성")
    @ParameterizedTest
    @ValueSource(strings = {MANUAL_TOPIC_NAME, MANUAL_IMMEDIATE_TOPIC_NAME, BATCH_TOPIC_NAME, COUNT_TOPIC_NAME})
    void testCreateTopic(String topicNm) {
        // given
        NewTopic newTopic = TopicBuilder.name(topicNm).partitions(2).replicas(1).build();

        // when,then
        Assertions.assertDoesNotThrow(() -> adminClient.createTopics(Collections.singletonList(newTopic)).all().get());

    }

    @DisplayName("테스트 토픽 삭제")
    @Test
    void deleteTopic() {
        // given
        List<String> topics = Arrays.asList(MANUAL_TOPIC_NAME, MANUAL_IMMEDIATE_TOPIC_NAME, BATCH_TOPIC_NAME, COUNT_TOPIC_NAME);

        // when,then
        Assertions.assertDoesNotThrow(() -> adminClient.deleteTopics(topics).all().get());
    }

    private static Stream<Arguments> ackModeTestSource() {
        return Stream.of(
                Arguments.of(MANUAL_IMMEDIATE_TOPIC_NAME, MANUAL_IMMEDIATE_GROUP_NAME, ContainerProperties.AckMode.MANUAL_IMMEDIATE, 5),
                Arguments.of(MANUAL_TOPIC_NAME, MANUAL_GROUP_NAME, ContainerProperties.AckMode.MANUAL, 1)
        );
    }

    /**
     * AckMode에 따른 commit 테스트
     * <pre>
     *     ackMode
     *      - MANUAL
     *          - 데이터 조회 후 acknowledgment.acknowledge() 호출시 한번에 commit
     *      - MANUAL_IMMEDIATE_GROUP_NAME
     *          - 데이터 조회 후 acknowledgment.acknowledge() 호출시 즉시 commit
     * </pre>
     */
    @DisplayName("ackMode > MANUAL / MANUAL_IMMEDIATE_GROUP_NAME commit 테스트")
    @ParameterizedTest
    @MethodSource("ackModeTestSource")
    void testAckMode(String topicName, String groupName,
                     ContainerProperties.AckMode ackMode,
                     int expectCommitCount) throws InterruptedException {
        // given
        AtomicInteger commitCount = new AtomicInteger();
        AcknowledgingMessageListener<String, String> messageListener = (data, acknowledgment) -> {
            log.info("> Consumer onMessage > topic : {}, key:{}, value:{} , offset:{}", data.topic(), data.key(), data.value(), data.offset());
            acknowledgment.acknowledge();
        };

        ContainerProperties containerProps = new ContainerProperties(topicName);
        containerProps.setGroupId(groupName);
        containerProps.setAckMode(ackMode);
        containerProps.setMessageListener(messageListener);
        containerProps.setCommitLogLevel(LogIfLevelEnabled.Level.DEBUG);
        containerProps.setLogContainerConfig(true);
        containerProps.setSyncCommits(false);
        containerProps.setCommitCallback((offsets, exception) -> offsets.forEach((key, value) -> {
            log.info(">>>> onComplete topic:{}, partition:{}, offset:{}, leaderEpoch:{}", key.topic(), key.partition(), value.offset(), value.leaderEpoch());
            // 토픽내 offset 초기화 경우 skip
            if (value.offset() > 0) {
                commitCount.getAndIncrement();
            }
        }));

        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(configs);
        KafkaMessageListenerContainer<String, String> kafkaMessageListenerContainer = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        kafkaMessageListenerContainer.start();

        // when
        this.setTestData(5, topicName);

        Thread.sleep(3000);

        // then
        assertEquals(expectCommitCount, commitCount.get());
    }

    /**
     * listener로 데이터 전달되면 내부적으로 조회한 record의 offset 정보를 모았다가 한번에 commit(MANUAL 모드의 acknowledgement.acknowledge() 호출시와 유사)
     * ex) topic에 데이터 10개 있고 MAX_POLL_RECORDS_CONFIG 값이 5인 경우, 2번 조회되며 commit은 2번 실행
     */
    @DisplayName("ackMode > BATCH commit 테스트")
    @Test
    void testAckModeBatch() throws InterruptedException {
        // given
        AtomicInteger commitCount = new AtomicInteger();
        BatchAcknowledgingMessageListener<String, String> messageListener = (data, acknowledgment) -> {
            data.forEach(info -> log.info("Consumer onMessage > topic : {}, key:{}, value:{} , offset:{}", info.topic(), info.key(), info.value(), info.offset()));
        };

        ContainerProperties containerProps = new ContainerProperties(BATCH_TOPIC_NAME);
        containerProps.setGroupId(BATCH_GROUP_NAME);
        containerProps.setAckMode(ContainerProperties.AckMode.BATCH);
        containerProps.setMessageListener(messageListener);
        containerProps.setCommitLogLevel(LogIfLevelEnabled.Level.DEBUG);
        containerProps.setLogContainerConfig(true);
        containerProps.setSyncCommits(false);
        containerProps.setCommitCallback((offsets, exception) -> offsets.forEach((key, value) -> {
            log.info(">>>> onComplete topic:{}, partition:{}, offset:{}, leaderEpoch:{}", key.topic(), key.partition(), value.offset(), value.leaderEpoch());
            // 토픽내 offset 초기화 경우 skip
            if (value.offset() > 0) {
                commitCount.getAndIncrement();
            }
        }));

        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);

        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(configs);
        KafkaMessageListenerContainer<String, String> kafkaMessageListenerContainer = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        kafkaMessageListenerContainer.start();

        // when
        this.setTestData(6, BATCH_TOPIC_NAME);

        Thread.sleep(3000);

        assertEquals(ContainerProperties.AckMode.BATCH, containerProps.getAckMode());
        assertEquals(2, commitCount.get());
    }

    private void setTestData(int repeatCount, String topicName) {
        LongStream.rangeClosed(1, repeatCount).forEach(prodNo -> {
            SampleSendDto sampleSendDto = SampleSendDto.builder().prodNo(prodNo).prodNm(topicName + " testProdNo " + prodNo).build();

            Assertions.assertDoesNotThrow(() -> kafkaTemplate.send(topicName, "testKey", sampleSendDto)
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
}
