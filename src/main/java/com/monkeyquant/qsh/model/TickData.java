package com.monkeyquant.qsh.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.sql.Timestamp;

@Getter
@Setter
@Builder
public class TickData {
  private MarketDealType dealType;
  private String instrument;
  private double price;
  private int volume;
  private Timestamp time;
  private long dealId;
}
