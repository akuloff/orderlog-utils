package com.monkeyquant.qsh.model;

import com.alex09x.qsh.reader.type.OrdersLogRecord;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@Builder
public class OrdersMap {
  Map<Long, OrdersLogRecord> ordersMap;
}
