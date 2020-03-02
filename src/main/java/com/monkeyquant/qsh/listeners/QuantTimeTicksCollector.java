package com.monkeyquant.qsh.listeners;

import com.monkeyquant.jte.primitives.interfaces.ITickData;
import lombok.Getter;

@Getter
public class QuantTimeTicksCollector {
  private int totalTicks;
  private int totalVolume;
  private double openPrice;
  private double highPrice;
  private double lowPrice;
  private double closePrice;
  private double avgPrice;
  private double totalPrice;
  private long startTime;

  public QuantTimeTicksCollector(ITickData t) {
    totalTicks = 1;
    totalVolume = t.getAmount();
    openPrice = highPrice = lowPrice = closePrice = avgPrice = totalPrice = t.getPrice();
    startTime = t.getDate().getTime();
  }

  public void addTick(ITickData t) {
    totalTicks++;
    totalVolume += t.getAmount();
    double p = t.getPrice();
    closePrice = p;
    totalPrice += p;
    avgPrice = totalPrice / totalTicks;
    if (p > highPrice) {
      highPrice = p;
    } else if (p < lowPrice) {
      lowPrice = p;
    }
  }
}
