package com.kafkastudy.kafkastudy.consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class AllowAutoCreateTopicTest {

    @Autowired
    AdminClient adminClient;

    private static final String TOPIC_NAME = "test-auto-create-topic";
    private static final String GROUP_NAME = "test-auto-create-topic-group";

    /**
     * allow.auto.create.topics (default:true) 에 의해 토픽 구독시, 토픽 자동 생성
     * 브로커 auto.create.topics.enable (default:true)으로 토픽 자동 생성 허용
     */
    @DisplayName("allow.auto.create.topics 에 따른 토픽 자동 생성 테스트")
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAllowAutoCreateTopics(boolean isAllowAutoCreateTopics) throws ExecutionException, InterruptedException {
        // given
        Collection<TopicListing> beforeTopicListings = adminClient.listTopics().listings().get();
        boolean isNonExists = beforeTopicListings.stream().noneMatch(info -> info.name().equals(TOPIC_NAME));

        // 컨슈머 생성
        ContainerProperties containerProps = new ContainerProperties(TOPIC_NAME);
        containerProps.setGroupId(GROUP_NAME);
        containerProps.setMessageListener((AcknowledgingConsumerAwareMessageListener<String, String>) (data, acknowledgment, consumer) -> acknowledgment.acknowledge());
        containerProps.setAckMode(ContainerProperties.AckMode.MANUAL);

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
        configs.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, isAllowAutoCreateTopics);


        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(configs);
        KafkaMessageListenerContainer<String, String> firstContainer = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);

        // when
        firstContainer.start();

        Thread.sleep(1000);

        Collection<TopicListing> afterTopicListings = adminClient.listTopics().listings().get();
        boolean isExists = afterTopicListings.stream().anyMatch(info -> info.name().equals(TOPIC_NAME));
        if (isExists) {
            adminClient.deleteTopics(Collections.singletonList(TOPIC_NAME)).all().get();
        }

        firstContainer.stop();

        // then
        assertTrue(isNonExists);
        assertEquals(isExists, isAllowAutoCreateTopics);

    }
}
