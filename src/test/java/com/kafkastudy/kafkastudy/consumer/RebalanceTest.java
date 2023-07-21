package com.kafkastudy.kafkastudy.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
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
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.LogIfLevelEnabled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@SpringBootTest
public class RebalanceTest {

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired
    AdminClient adminClient;

    private static final String TOPIC_NAME = "test-consumer-group-topic";
    private static final String GROUP_NAME = "test-consumer-group";
    private static final String NEW_GROUP_NAME = "new-test-consumer-group";

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
     * 1. consumer1이 토픽의 전체 파티션 할당
     * 2. 2초뒤 동일 그룹에 newConsumer1 추가시 리밸런스 발생
     * 3. 전체 파티션에 대해 consumer1/newConsumer1 이 리밸런스에 의해 파티션 재 할당
     */
    @DisplayName("동일 그룹에 컨슈머 추가되는 경우 파티션 할당 확인")
    @Test
    void testRebalance() throws InterruptedException {
        // given
        List<Pair<String, String>> changedPartitions = new ArrayList<>();

        ContainerProperties containerProps = new ContainerProperties(TOPIC_NAME);
        containerProps.setGroupId(GROUP_NAME);
        containerProps.setMessageListener((AcknowledgingConsumerAwareMessageListener<String, String>) (data, acknowledgment, consumer) -> acknowledgment.acknowledge());
        containerProps.setAckMode(ContainerProperties.AckMode.MANUAL);
        containerProps.setCommitLogLevel(LogIfLevelEnabled.Level.DEBUG);
        containerProps.setLogContainerConfig(true);
        containerProps.setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                String assignedPartitions = partitions.stream().map(TopicPartition::partition).map(String::valueOf).collect(Collectors.joining(","));
                log.info("[{}] onPartitionsAssigned {}", Thread.currentThread().getName(), assignedPartitions);
                changedPartitions.add(Pair.of(Thread.currentThread().getName(), assignedPartitions));
            }
        });

        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(1);
        taskExecutor.setThreadNamePrefix("consumer");
        taskExecutor.initialize();
        containerProps.setConsumerTaskExecutor(taskExecutor);

        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);


        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(configs);
        KafkaMessageListenerContainer<String, String> firstContainer = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        firstContainer.start();
        Thread.sleep(2000);

        // when
        ThreadPoolTaskExecutor newTaskExecutor = new ThreadPoolTaskExecutor();
        newTaskExecutor.setCorePoolSize(1);
        newTaskExecutor.setThreadNamePrefix("newConsumer");
        newTaskExecutor.initialize();
        containerProps.setConsumerTaskExecutor(newTaskExecutor);
        KafkaMessageListenerContainer<String, String> secondContainer = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        secondContainer.start();

        Thread.sleep(3000);

        // then
        assertEquals(3, changedPartitions.size());
    }
}
