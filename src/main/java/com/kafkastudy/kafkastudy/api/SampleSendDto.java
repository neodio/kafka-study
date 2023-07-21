package com.kafkastudy.kafkastudy.api;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Setter
@Getter
@Builder
@AllArgsConstructor
public class SampleSendDto {
    @NotNull(message = "상품번호")
    private Long prodNo;

    @NotBlank(message = "상품명")
    private String prodNm;
}
