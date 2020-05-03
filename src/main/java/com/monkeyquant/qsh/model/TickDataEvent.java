package com.monkeyquant.qsh.model;

import com.monkeyquant.jte.primitives.interfaces.ITickData;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Data
@Builder
public class TickDataEvent {
  Instant time;
  ITickData tickData;
}
