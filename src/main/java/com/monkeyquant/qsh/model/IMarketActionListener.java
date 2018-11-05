package com.monkeyquant.qsh.model;

public interface IMarketActionListener {
  default void onBookChange(java.sql.Timestamp time){};
  default void onNewTick(TickData tickData){};
}
