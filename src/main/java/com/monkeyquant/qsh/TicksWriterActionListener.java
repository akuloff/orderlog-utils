package com.monkeyquant.qsh;

import com.monkeyquant.jte.primitives.interfaces.ITickData;
import com.monkeyquant.qsh.model.TickDataEvent;
import lombok.extern.log4j.Log4j;

import java.io.FileWriter;
import java.io.IOException;

@Log4j
public class TicksWriterActionListener extends MoscowTimeZoneActionListener {
  public TicksWriterActionListener(FileWriter writer, String dateFormat) {
    super(writer, dateFormat);
  }

  @Override
  public void onNewTick(TickDataEvent tickDataEvent) {
    try {
      ITickData tickData = tickDataEvent.getTickData();
      writer.write(String.format("%s;%s;%s;%s\n", formatter.format(tickDataEvent.getTime()), tickData.getPrice(), tickData.getAmount(), tickData.getTradeId()));
    } catch (IOException e) {
      log.error("write exception", e);
    }
  }
}
