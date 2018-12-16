package com.monkeyquant.qsh;

import com.monkeyquant.qsh.model.IMarketActionListener;
import lombok.extern.log4j.Log4j;

import java.io.FileWriter;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

@Log4j
public class MoscowTimeZoneActionListener implements IMarketActionListener {
  protected final FileWriter writer;
  protected SimpleDateFormat formatter;
  protected final Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT+3"));
  protected final SimpleDateFormat mqlDateFormat;
  protected final SimpleDateFormat mqlTimeFormat;
  protected int startTime = 600;
  protected int endTime = 1425;
  protected int scale = 2;

  protected boolean checkTime(Date date) {
    calendar.setTime(date);
    int ctime = calendar.get(Calendar.HOUR_OF_DAY) * 60 + calendar.get(Calendar.MINUTE);
    return ctime >= startTime && ctime <= endTime;
  }

  protected String summFormat(double value) {
    return new BigDecimal(value).setScale(scale, BigDecimal.ROUND_CEILING).toString();
  }

  public MoscowTimeZoneActionListener(FileWriter writer, String dateFormat, int scale, int startTime, int endTime) {
    this.writer = writer;
    formatter = new SimpleDateFormat(dateFormat);
    formatter.setCalendar(calendar);
    formatter.setTimeZone(calendar.getTimeZone());

    mqlDateFormat = new SimpleDateFormat("yyyy.MM.dd");
    mqlDateFormat.setCalendar(calendar);
    mqlDateFormat.setTimeZone(calendar.getTimeZone());
    mqlTimeFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    mqlTimeFormat.setCalendar(calendar);
    mqlTimeFormat.setTimeZone(calendar.getTimeZone());

    this.startTime = startTime;
    this.endTime = endTime;
    this.scale = scale;
  }
}
