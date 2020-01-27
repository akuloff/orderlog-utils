package com.monkeyquant.qsh.model;

import com.alex09x.qsh.reader.type.OrdersLogRecord;
import com.monkeyquant.jte.primitives.interfaces.IBookState;

public interface IOrdersProcessor {
  void processOrderRecord(OrdersLogRecord rec);
  default IBookState getBookState() {return null;};
  default void init() throws Exception{};
}
