package com.monkeyquant.qsh.listeners;

import com.monkeyquant.qsh.model.BookStateEvent;
import com.monkeyquant.qsh.model.IMarketActionListener;
import com.monkeyquant.qsh.model.TickDataEvent;
import lombok.Getter;
import lombok.ToString;

@ToString
public class StatsMarketActionListener implements IMarketActionListener {

  @Getter
  private long totalBookStates = 0;

  @Getter
  private long totalTicks = 0;

  @Override
  public void onBookChange(BookStateEvent bookStateEvent) {
    totalBookStates ++;
  }

  @Override
  public void onNewTick(TickDataEvent tickData) {
    totalTicks ++;
  }
}
