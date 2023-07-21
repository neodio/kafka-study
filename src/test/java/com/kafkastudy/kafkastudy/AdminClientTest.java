package com.kafkastudy.kafkastudy;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class AdminClientTest {

    @Autowired
    AdminClient adminClient;

    private static final String TOPIC_NAME = "test-topic";

    @DisplayName("topic 생성")
    @Test
    void testCreateTopic() throws ExecutionException, InterruptedException {
        // given
        NewTopic newTopic = TopicBuilder.name(TOPIC_NAME).partitions(1).replicas(1).build();

        // when
        KafkaFuture<Void> kafkaFuture = adminClient.createTopics(Collections.singletonList(newTopic)).all();
        kafkaFuture.get();

        assertFalse(kafkaFuture.isCompletedExceptionally());
        assertTrue(kafkaFuture.isDone());
    }

    @DisplayName("토픽 정보 조회")
    @Test
    void testDescribeTopics() throws ExecutionException, InterruptedException {
        // given

        // when
        KafkaFuture<Map<String, TopicDescription>> kafkaFuture = adminClient.describeTopics(Collections.singletonList(TOPIC_NAME)).all();
        Map<String, TopicDescription> stringTopicDescriptionMap = kafkaFuture.get();

        // then
        assertAll(
                () -> assertNotNull(stringTopicDescriptionMap),
                () -> assertEquals(TOPIC_NAME, stringTopicDescriptionMap.get(TOPIC_NAME).name())
        );
    }

}
