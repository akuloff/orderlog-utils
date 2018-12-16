package com.monkeyquant.qsh;

import com.monkeyquant.jte.primitives.interfaces.ITickData;
import com.monkeyquant.qsh.model.TickDataEvent;
import lombok.extern.log4j.Log4j;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;

@Log4j
public class TicksWriterActionListener extends MoscowTimeZoneActionListener {
  private final boolean useMql;

  public TicksWriterActionListener(FileWriter writer, String dateFormat, String timeFormat, boolean useMql, int scale, int startTime, int endTime) {
    super(writer, dateFormat, timeFormat, scale, startTime, endTime);
    this.useMql = useMql;
  }

  @Override
  public void onNewTick(TickDataEvent tickDataEvent) {
    try {
      ITickData tickData = tickDataEvent.getTickData();
      Timestamp tickTime = tickDataEvent.getTime();
      if (checkTime(tickDataEvent.getTime())) {
        if (useMql) {
          writer.write(String.format("%s;%s;%s;%s;%s;%s\n",
            mqlDateFormat.format(tickTime), mqlTimeFormat.format(tickTime),
            summFormat(tickData.getPrice()), summFormat(tickData.getPrice()), summFormat(tickData.getPrice()), tickData.getAmount()));
        } else {
          writer.write(String.format("%s;%s;%s;%s\n", dateFormat.format(tickTime), summFormat(tickData.getPrice()), tickData.getAmount(), tickData.getTradeId()));
        }
      }
    } catch (IOException e) {
      log.error("write exception", e);
    }
  }
}
