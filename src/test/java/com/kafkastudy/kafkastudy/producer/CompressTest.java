package com.kafkastudy.kafkastudy.producer;

import com.google.gson.Gson;
import com.kafkastudy.kafkastudy.api.SampleSendDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.record.BufferSupplier;
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
import org.springframework.kafka.support.LogIfLevelEnabled;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@SpringBootTest
public class CompressTest {

    @Autowired
    AdminClient adminClient;

    KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private static final String TOPIC_NAME = "test-compress-topic";
    private static final String GROUP_NAME = "test-compress-group";

    @BeforeEach
    void init() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

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
     * 압축 옵션을 통해 데이터 등록시 압축한 경우, consumer는 자동으로 압축 여부에 따라 데이터 압축 해제함
     * 참고 {@link org.apache.kafka.common.record.DefaultRecordBatch#streamingIterator(BufferSupplier)
     */
    @DisplayName("compression.type에 따른 압축 해제 확인")
    @Test
    void testCompressType() throws InterruptedException {

        // given
        ContainerProperties containerProps = new ContainerProperties(TOPIC_NAME);
        containerProps.setGroupId(GROUP_NAME);
        containerProps.setMessageListener((MessageListener<String, String>) data -> log.info("> Consumer onMessage > topic : {}, key:{}, value:{} , offset:{}", data.topic(), data.key(), data.value(), data.offset()));
        containerProps.setCommitLogLevel(LogIfLevelEnabled.Level.DEBUG);
        containerProps.setLogContainerConfig(true);
        containerProps.setSyncCommits(false);

        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(configs);
        KafkaMessageListenerContainer<String, String> kafkaMessageListenerContainer = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        kafkaMessageListenerContainer.start();


        SampleSendDto sampleSendDto = SampleSendDto.builder().prodNo(3L).prodNm("2").build();

        // when
        kafkaTemplate.send(TOPIC_NAME, sampleSendDto)
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

        Thread.sleep(3000);

        // then 에러없으면 정상
    }
}
