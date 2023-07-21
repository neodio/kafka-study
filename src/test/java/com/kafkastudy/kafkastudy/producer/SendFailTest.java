package com.kafkastudy.kafkastudy.producer;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

@Slf4j
@SpringBootTest
public class SendFailTest {

    @Mock
    Producer<String, Object> producer;

    @Mock
    ProducerFactory<String, Object> producerFactory;

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @DisplayName("메세지 등록 실패 테스트")
    @Test
    void testSendFail() throws InterruptedException {
        // given
        String data = "hello";
        AtomicReference<String> failData = new AtomicReference<>();

        given(producer.send(any(), any())).willAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                Callback callback = invocation.getArgument(1);
                callback.onCompletion(null, new RuntimeException("test"));
                return new SettableListenableFuture<RecordMetadata>();
            }
        });

        given(producerFactory.createProducer()).willReturn(producer);

        KafkaTemplate<String, Object> template = new KafkaTemplate<>(producerFactory);

        Consumer<ProducerRecord<String, String>> consumer = (producerRecord) -> kafkaTemplate.send(producerRecord.topic(), producerRecord.value())
                .addCallback(new KafkaSendCallback<String, Object>() {
                    @Override
                    public void onFailure(KafkaProducerException ex) {
                        // 통합 로그 로깅 처리
                    }

                    @Override
                    public void onSuccess(SendResult<String, Object> result) {
                        log.info("retry send onSuccess : {}", new Gson().toJson(result.getRecordMetadata()));
                    }
                });

        // when
        template.send("test-topic", data).addCallback(new KafkaSendCallback<String, Object>() {

            @Override
            public void onFailure(KafkaProducerException ex) {
                log.error("Send Fail! topic:{},value:{}", ex.getFailedProducerRecord().topic(), ex.getFailedProducerRecord().value());
                failData.set(ex.getFailedProducerRecord().value().toString());
                consumer.accept(ex.getFailedProducerRecord());
            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {
            }
        });

        // then
        assertEquals(failData.get(), data);

        Thread.sleep(2000);
    }
}
