package com.monkeyquant.qsh;

import com.monkeyquant.qsh.model.IMarketActionListener;
import lombok.extern.log4j.Log4j;

import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

@Log4j
public class MoscowTimeZoneActionListener implements IMarketActionListener {
  protected final FileWriter writer;
  protected SimpleDateFormat formatter;
  protected final Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT+3"));

  public MoscowTimeZoneActionListener(FileWriter writer, String dateFormat) {
    this.writer = writer;
    formatter = new SimpleDateFormat(dateFormat);
    formatter.setCalendar(calendar);
    formatter.setTimeZone(calendar.getTimeZone());
  }
}
