package com.kafkastudy.kafkastudy;

import com.google.gson.Gson;
import com.kafkastudy.kafkastudy.api.SampleSendDto;
import com.kafkastudy.kafkastudy.producer.SampleProducer;
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
public class ProducerTest {

    @Autowired
    SampleProducer sampleProducer;
    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @DisplayName("send 테스트")
    @Test
    void test() {
        // given
        SampleSendDto sampleSendDto = SampleSendDto.builder().prodNo(1L).prodNm("2").build();

        // when , then
        Assertions.assertDoesNotThrow(() -> kafkaTemplate.send("test0923", "testKey", sampleSendDto)
                .addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
                    @Override
                    public void onFailure(Throwable ex) {
                        ex.printStackTrace();
                    }

                    @Override
                    public void onSuccess(SendResult<String, Object> result) {
                        log.info("send onSuccess : {}", new Gson().toJson(result.getRecordMetadata()));
                    }
                }));

    }
}
