package com.kafkastudy.kafkastudy;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@SpringBootTest
public class ReactiveTest {

    @Autowired
    AdminClient adminClient;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    ReactiveKafkaProducerTemplate<String, String> reactiveKafkaProducerTemplate;
    ReactiveKafkaConsumerTemplate<Object, Object> reactiveKafkaConsumerTemplate;

    private static final String TOPIC_NAME = "test-reactive-topic";

    @BeforeEach
    void init() {
        Map<String, Object> producerConfigs = new HashMap<>();
        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        SenderOptions<String, String> senderOptions = SenderOptions.create(producerConfigs);
        reactiveKafkaProducerTemplate = new ReactiveKafkaProducerTemplate<>(senderOptions);


        Map<String, Object> consumerConfigs = new HashMap<>();
        consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup");
        consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        reactiveKafkaConsumerTemplate = new ReactiveKafkaConsumerTemplate<>(
                ReceiverOptions.create(consumerConfigs).subscription(Collections.singletonList(TOPIC_NAME))
        );
    }

    @DisplayName("테스트 토픽 생성")
    @Test
    void createTopic() throws ExecutionException, InterruptedException {
        // given
        NewTopic newTopic = TopicBuilder.name(TOPIC_NAME).partitions(1).replicas(1).build();

        // when
        KafkaFuture<Void> kafkaFuture = adminClient.createTopics(Collections.singletonList(newTopic)).all();
        kafkaFuture.get();

        // then
        assertFalse(kafkaFuture.isCompletedExceptionally());
        assertTrue(kafkaFuture.isDone());
    }

    @DisplayName("1초 간격으로 메세지 등록")
    @Test
    void testReactiveKafkaSender() throws InterruptedException {
        // given
        Flux<SenderRecord<String, String, Integer>> senderRecordFlux = Flux.range(1, 5)
                .map(idx -> SenderRecord.create(new ProducerRecord<>(TOPIC_NAME, "test " + idx), idx));

        AtomicInteger count = new AtomicInteger();
        // when
        reactiveKafkaProducerTemplate.send(senderRecordFlux)
                .delayElements(Duration.ofSeconds(1))
                .doOnNext(senderResult -> {
                    count.getAndIncrement();
                    log.info("sendResult offset:{}, idx:{}", senderResult.recordMetadata().offset(), senderResult.correlationMetadata());
                })
                .log()
                .subscribe();

        Thread.sleep(6000);

        // then
        assertEquals(5, count.get());
    }

    @DisplayName("0.5초 간격으로 메세지 조회")
    @Test
    void testReactiveKafkaReceiver() throws InterruptedException {
        // given
        Flux<SenderRecord<String, String, Integer>> senderRecordFlux = Flux.range(1, 5)
                .map(idx -> SenderRecord.create(new ProducerRecord<>(TOPIC_NAME, "test " + idx), idx));

        reactiveKafkaProducerTemplate.send(senderRecordFlux)
                .doOnNext(senderResult -> {
                    log.info("[producer] sendResult offset:{}, idx:{}", senderResult.recordMetadata().offset(), senderResult.correlationMetadata());
                })
                .subscribe();

        Thread.sleep(2000);

        // when
        AtomicInteger count = new AtomicInteger();
        reactiveKafkaConsumerTemplate.receiveAutoAck()
                .delayElements(Duration.ofMillis(500))
                .doOnNext(record -> {
                    count.getAndIncrement();
                    log.info("[consumer] received value: {}, offset:{}", record.value(), record.offset());
                })
                .subscribe();

        Thread.sleep(6000);

        // then
        assertEquals(5, count.get());
    }
}
