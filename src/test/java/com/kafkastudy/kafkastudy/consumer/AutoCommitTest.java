package com.kafkastudy.kafkastudy.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
@SpringBootTest
class AutoCommitTest {

    /**
     * 1. auto commit 일 경우
     * 2. 60초마다 브로커에게 오프셋을 전달한다. (커밋한다.)
     * 3. 커밋이 되기전에 장애가 발생하여 컨슈머가 죽는다면?
     * 4. 커밋이 되지 않았기 때문에 메세지를 중복하여 처리되는 문제가 발생함.
     */
    @Test
    void auto_commit() {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id-test");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true); // auto commit
        configs.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 60000); // 1분

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList("test_topic"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("value: {}", record.value());
            }
        }
    }

    /**
     * 1. auto commit을 끄고 수동으로 커밋한다.
     * 2. commit을 동기화 처리한다.
     * 3. 메세지를 소비하면서 커밋을 처리함.
     * 4. 컨슈밍 중 컨슈머를 죽이면?
     * 5. 커밋이 그전까지 완료되었으니 재실행해도 그전커밋부터 실행함.
     * 6. 중복현상 발생하지 않음.
     */
    @Test
    void commit_sync() {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id-test");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // non-auto commit

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList("test_topic"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("value: {}", record.value());
            }
            try {
                consumer.commitSync(); // 동기화
            } catch (CommitFailedException e) {
                log.error("commit failed : {}", e.getMessage());
            }
        }
    }
}
