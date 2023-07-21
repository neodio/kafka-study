package com.kafkastudy.kafkastudy.api.controller;

import com.kafkastudy.kafkastudy.api.SampleSendDto;
import com.kafkastudy.kafkastudy.constants.CodeConstants;
import com.kafkastudy.kafkastudy.producer.SampleProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

@RestController
@RequiredArgsConstructor
public class SampleController {

    private final SampleProducer sampleProducer;

    @ResponseStatus(HttpStatus.OK)
    @PostMapping("/send")
    public void sampleSendData(@Valid @RequestBody SampleSendDto sampleSendDto) {
        sampleProducer.send(CodeConstants.BATCH_TOPIC_NAME, sampleSendDto);
    }
}
