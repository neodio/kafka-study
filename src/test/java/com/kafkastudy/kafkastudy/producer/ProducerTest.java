package com.kafkastudy.kafkastudy.producer;

import com.google.gson.Gson;
import com.kafkastudy.kafkastudy.api.SampleSendDto;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Slf4j
@SpringBootTest
class ProducerTest {

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @DisplayName("특정 토픽에 key없이 value만 전송하면 여러 파티션에 round-robin 방식으로 적재된다. (커넥션 단위로)")
    @Test
    void key_null() {
        for (int i = 1; i <= 20; i++) {
            SampleSendDto sampleSendDto = SampleSendDto.builder()
                    .prodNo((long) i)
                    .prodNm("상품" + i)
                    .build();

            Assertions.assertDoesNotThrow(() -> kafkaTemplate.send("test_topic", sampleSendDto)
                    .addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
                        @Override
                        public void onFailure(Throwable ex) {
                            log.error("send onSuccess : {}", new Gson().toJson(ex.getMessage()));
                        }

                        @Override
                        public void onSuccess(SendResult<String, Object> result) {
                            log.info("send onSuccess : {}", new Gson().toJson(result.getRecordMetadata()));
                        }
                    }));
        }
    }

    @DisplayName("특정 토픽에 key, value를 전송하면 파티션과 1대1 매핑되어 적재한다.")
    @Test
    void key_value() {
        for (int i = 1; i <= 20; i++) {
            SampleSendDto sampleSendDto = SampleSendDto.builder()
                    .prodNo((long) i)
                    .prodNm("상품" + i)
                    .build();

            Assertions.assertDoesNotThrow(() -> kafkaTemplate.send("test_topic", "1", sampleSendDto)
                    .addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
                        @Override
                        public void onFailure(Throwable ex) {
                            log.error("send onSuccess : {}", new Gson().toJson(ex.getMessage()));
                        }

                        @Override
                        public void onSuccess(SendResult<String, Object> result) {
                            log.info("send onSuccess : {}", new Gson().toJson(result.getRecordMetadata()));
                        }
                    }));
        }
    }
}
