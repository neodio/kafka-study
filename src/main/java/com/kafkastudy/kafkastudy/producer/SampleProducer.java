package com.kafkastudy.kafkastudy.producer;

import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Slf4j
@Service
@RequiredArgsConstructor
public class SampleProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public <T> void send(String topicNm, T data) {

        kafkaTemplate.send(topicNm, data)
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

    }

}
