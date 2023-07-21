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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@SpringBootTest
public class PartitionTest {

    @Autowired
    AdminClient adminClient;
    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TOPIC_NAME = "test-partition-topic";
    private static final String GROUP_NAME = "test-partition-group";

    @DisplayName("테스트 토픽 생성")
    @Test
    void testCreateTopic() {
        // given
        NewTopic newTopic = TopicBuilder.name(TOPIC_NAME).partitions(2).replicas(1).build();

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

    @DisplayName("특정 파티션 조회")
    @Test
    void testSpecificPartition() throws InterruptedException {
        // given
        Set<Integer> partitionSet = new HashSet<>();
        ContainerProperties containerProps = new ContainerProperties(new TopicPartitionOffset(TOPIC_NAME, 0));
        containerProps.setGroupId(GROUP_NAME);
        containerProps.setMessageListener((MessageListener<String, String>) data -> {
            partitionSet.add(data.partition());
            log.info("> Consumer onMessage > partition : {}, key:{}, value:{} , offset:{}", data.partition(), data.key(), data.value(), data.offset());
        });

        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(configs);
        KafkaMessageListenerContainer<String, String> firstContainer = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        firstContainer.start();

        // when
        LongStream.rangeClosed(1, 10).forEach(prodNo -> {
            SampleSendDto sampleSendDto = SampleSendDto.builder().prodNo(prodNo).prodNm(TOPIC_NAME + " testProdNo " + prodNo).build();

            kafkaTemplate.send(TOPIC_NAME, String.valueOf(prodNo), sampleSendDto)
                    .addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
                        @Override
                        public void onFailure(Throwable ex) {
                            ex.printStackTrace();
                        }

                        @Override
                        public void onSuccess(SendResult<String, Object> result) {
                            log.info("send onSuccess : {}", new Gson().toJson(result.getRecordMetadata()));
                        }
                    });
        });

        Thread.sleep(2000);

        // then
        assertEquals(1, partitionSet.size());
    }
}
