package com.monkeyquant.qsh.model;

public interface IMarketActionListener {
  default void onBookChange(BookStateEvent bookStateEvent) throws Exception{};
  default void onNewTick(TickDataEvent tickDataEvent) throws Exception{};
  default void init() throws Exception {};
}
