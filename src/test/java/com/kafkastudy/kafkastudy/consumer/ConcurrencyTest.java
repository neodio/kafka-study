package com.kafkastudy.kafkastudy.consumer;

import com.google.gson.Gson;
import com.kafkastudy.kafkastudy.api.SampleSendDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.LogIfLevelEnabled;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("consumer concurrency 옵션 테스트")
@Slf4j
@SpringBootTest
public class ConcurrencyTest {

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired
    AdminClient adminClient;

    private static final String TOPIC_NAME = "test-concurrency";
    private static final String GROUP_NAME = "test-concurrency-group";

    @DisplayName("테스트 토픽 생성")
    @Test
    void testCreateTopic() {
        // given
        NewTopic newTopic = TopicBuilder.name(TOPIC_NAME).partitions(3).replicas(1).build();

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
     * concurrency 값 만큼 리스너 컨테이너 생성 {@link ConcurrentMessageListenerContainer#doStart()} }
     * <pre>
     * concurrency = partitionCount
     *  - 컨슈머 별로 파티션 1개씩 할당
     * concurrency < partitionCount
     *  - 컨슈머 별로 할당 받은 파티션 수 다름
     * concurrency > partitionCount
     *  - 파티션 할당 못받은 컨슈머 존재 >> 리소스 낭비
     * </pre>
     */
    @DisplayName("consumer concurrency 옵션 테스트")
    @Test
    void testConcurrency() throws InterruptedException {
        // given
        int concurrency = 3;
        // key : consumer thread name , value : assigned partition no
        Map<String, List<Integer>> consumerMap = new HashMap<>();

        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configs.put(ConsumerConfig.CLIENT_ID_CONFIG, "concurrency-consumer");

        ContainerProperties containerProps = new ContainerProperties(TOPIC_NAME);
        containerProps.setGroupId(GROUP_NAME);
        containerProps.setAckMode(ContainerProperties.AckMode.MANUAL);
        containerProps.setCommitLogLevel(LogIfLevelEnabled.Level.DEBUG);
        containerProps.setLogContainerConfig(true);
        containerProps.setSyncCommits(false);
        containerProps.setCommitCallback((offsets, exception) -> offsets.forEach((key, value) -> {
            log.info(">>>> onComplete topic:{}, partition:{}, offset:{}, leaderEpoch:{}", key.topic(), key.partition(), value.offset(), value.leaderEpoch());
        }));

        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(concurrency);
        taskExecutor.setThreadNamePrefix("consumer");
        taskExecutor.initialize();
        containerProps.setConsumerTaskExecutor(taskExecutor);

        ConcurrentMessageListenerContainer<String, String> container = new ConcurrentMessageListenerContainer<>(new DefaultKafkaConsumerFactory<>(configs), containerProps);
        container.setConcurrency(concurrency);
        container.setupMessageListener((AcknowledgingConsumerAwareMessageListener<String, String>) (data, acknowledgment, consumer) -> {
            acknowledgment.acknowledge();

            consumerMap.computeIfAbsent(
                    Thread.currentThread().getName(), key -> consumer.assignment().stream().map(TopicPartition::partition).collect(Collectors.toList())
            );
        });
        container.start();

        // when
        this.setTestData();

        Thread.sleep(3000);

        // then
        assertEquals(concurrency, consumerMap.size());
        consumerMap.forEach((key, value) -> assertEquals(1, value.size()));

    }

    public void setTestData() {
        LongStream.rangeClosed(1, 4).forEach(prodNo -> {
            SampleSendDto sampleSendDto = SampleSendDto.builder().prodNo(prodNo).prodNm(TOPIC_NAME + " testProdNo " + prodNo).build();

            Assertions.assertDoesNotThrow(() -> kafkaTemplate.send(TOPIC_NAME, String.valueOf(prodNo), sampleSendDto)
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
