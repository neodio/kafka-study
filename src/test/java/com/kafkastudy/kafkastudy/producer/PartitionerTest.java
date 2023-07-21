package com.kafkastudy.kafkastudy.producer;

import com.google.gson.Gson;
import com.kafkastudy.kafkastudy.api.SampleSendDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

@Slf4j
@SpringBootTest
public class PartitionerTest {
    @Autowired
    AdminClient adminClient;

    KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private static final String TOPIC_NAME = "test-partitioner-topic";
    private static final String GROUP_NAME = "test-partitioner-group";

    @BeforeEach
    void init() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);

        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    @DisplayName("테스트 토픽 생성")
    @Test
    void testCreateTopic() {
        // given
        NewTopic newTopic = TopicBuilder.name(TOPIC_NAME).partitions(2).replicas(2).build();

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
     * custom partitioner {@link CustomPartitioner}를 통해 1번 파티션에만 데이터 등록
     * <p>
     * - default partitioner {@link org.apache.kafka.clients.producer.internals.DefaultPartitioner}
     * - custom partitioner 설정 대신 kafkaTemplate 을 통해 특정 파티션에 데이터 등록 가능
     */
    @DisplayName("프로듀서 설정시 정의된 Partitioner 를 통해 저장될 파티션 확인")
    @Test
    void testPartitioner() throws InterruptedException {
        // given
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        ContainerProperties containerProps = new ContainerProperties(TOPIC_NAME);
        containerProps.setGroupId(GROUP_NAME);
        containerProps.setMessageListener((MessageListener<String, String>) data -> log.info("onMessage partition : {}, key:{}, value:{} ", data.partition(), data.key(), data.value()));

        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(configs);
        KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        container.start();

        // when
        LongStream.rangeClosed(1, 10).forEach(prodNo -> {
            SampleSendDto sampleSendDto = SampleSendDto.builder().prodNo(prodNo).prodNm(TOPIC_NAME + " testProdNo " + prodNo).build();
            kafkaTemplate.send(TOPIC_NAME, String.valueOf(prodNo), sampleSendDto).addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
                @Override
                public void onFailure(Throwable ex) {

                }

                @Override
                public void onSuccess(SendResult<String, Object> result) {
                    log.info("[{}] send onSuccess : {}", Thread.currentThread().getName(), new Gson().toJson(result.getRecordMetadata()));
                }
            });

        });

        Thread.sleep(3000);

        // then >> 에러없으면 정상
    }

}

