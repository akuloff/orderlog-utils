package com.monkeyquant.qsh.model;

import com.alex09x.qsh.reader.type.OrdersLogRecord;

public interface IOrdersProcessor {
  void processOrderRecord(OrdersLogRecord rec);
  default IBookState getBookState() {return null;};
}
