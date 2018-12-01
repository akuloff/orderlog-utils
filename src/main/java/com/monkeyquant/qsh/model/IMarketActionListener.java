package com.monkeyquant.qsh.model;

public interface IMarketActionListener {
  default void onBookChange(BookStateEvent bookStateEvent){};
  default void onNewTick(TickDataEvent tickDataEvent){};
}
