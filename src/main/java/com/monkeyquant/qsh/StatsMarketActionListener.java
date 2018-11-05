package com.monkeyquant.qsh;

import com.monkeyquant.qsh.model.IMarketActionListener;
import com.monkeyquant.qsh.model.TickData;
import lombok.Getter;
import lombok.ToString;

import java.sql.Timestamp;

@ToString
public class StatsMarketActionListener implements IMarketActionListener {

  @Getter
  private long totalBookStates = 0;

  @Getter
  private long totalTicks = 0;

  @Override
  public void onBookChange(Timestamp time) {
    totalBookStates ++;
  }

  @Override
  public void onNewTick(TickData tickData) {
    totalTicks ++;
  }
}
