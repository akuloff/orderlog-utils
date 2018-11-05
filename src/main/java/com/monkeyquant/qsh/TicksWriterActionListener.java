package com.monkeyquant.qsh;

import com.monkeyquant.qsh.model.IMarketActionListener;
import com.monkeyquant.qsh.model.TickData;
import lombok.extern.log4j.Log4j;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

@Log4j
public class TicksWriterActionListener implements IMarketActionListener {
  private final FileWriter writer;
  private SimpleDateFormat formatter;
  private final Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT+3"));

  public TicksWriterActionListener(FileWriter writer, String dateFormat) {
    this.writer = writer;
    formatter = new SimpleDateFormat(dateFormat);
    formatter.setCalendar(calendar);
    formatter.setTimeZone(calendar.getTimeZone());
  }

  @Override
  public void onNewTick(TickData tickData) {
    try {
      writer.write(String.format("%s;%s;%s;%s\n", formatter.format(tickData.getTime()), tickData.getPrice(), tickData.getVolume(), tickData.getDealId()));
    } catch (IOException e) {
      log.error("write exception", e);
    }
  }
}
