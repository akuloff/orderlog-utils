package com.monkeyquant.qsh.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@Builder
public class PriceRecord {
  private Double price;
  private Integer value;
}
