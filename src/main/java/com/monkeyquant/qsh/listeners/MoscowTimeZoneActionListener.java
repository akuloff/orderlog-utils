package com.monkeyquant.qsh.listeners;

import com.monkeyquant.qsh.application.ConverterParameters;
import com.monkeyquant.qsh.model.IMarketActionListener;
import com.monkeyquant.qsh.model.TimeFilter;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang3.StringUtils;

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
  protected SimpleDateFormat dateFormat;
  protected SimpleDateFormat timeFormat = null;
  protected final Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT+3"));
  protected final SimpleDateFormat mqlDateFormat;
  protected final SimpleDateFormat mqlTimeFormat;
  protected int startTime = 600;
  protected int endTime = 1425;
  protected int scale = 2;
  protected final TimeFilter timeFilter;
  protected ConverterParameters converterParameters;

  protected int getTimeCounter(Date date) {
    calendar.setTime(date);
    return calendar.get(Calendar.HOUR_OF_DAY) * 60 + calendar.get(Calendar.MINUTE);
  }

  protected boolean fortsTimeCheck(Date date){
    int ct = getTimeCounter(date);
    return !(ct >= (13*60 + 55) && ct <= 14*60 + 5) && !(ct >= (18*60 + 40) && ct <= 19*60 + 15);
  }

  protected boolean checkTime(Date date) {
    int ctime = getTimeCounter(date);
    boolean rval =  ctime >= startTime && ctime <= endTime;
    if (rval && timeFilter != TimeFilter.NONE) {
      switch (timeFilter) {
        case FORTS:
          rval = fortsTimeCheck(date);
          break;
      }
    }
    return rval;
  }

  protected String summFormat(double value) {
    return new BigDecimal(value).setScale(scale, BigDecimal.ROUND_CEILING).toString();
  }

  public MoscowTimeZoneActionListener(FileWriter writer, String dateFormat, String timeFormat, int scale, int startTime, int endTime, TimeFilter timeFilter) {
    this.writer = writer;
    this.dateFormat = new SimpleDateFormat(dateFormat);
    this.dateFormat.setCalendar(calendar);
    this.dateFormat.setTimeZone(calendar.getTimeZone());

    if (!StringUtils.isEmpty(timeFormat)) {
      this.timeFormat = new SimpleDateFormat(timeFormat);
      this.timeFormat.setCalendar(calendar);
      this.timeFormat.setTimeZone(calendar.getTimeZone());
    }

    mqlDateFormat = new SimpleDateFormat("yyyy.MM.dd");
    mqlDateFormat.setCalendar(calendar);
    mqlDateFormat.setTimeZone(calendar.getTimeZone());
    mqlTimeFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    mqlTimeFormat.setCalendar(calendar);
    mqlTimeFormat.setTimeZone(calendar.getTimeZone());

    this.startTime = startTime;
    this.endTime = endTime;
    this.scale = scale;
    this.timeFilter = timeFilter;
  }

  public MoscowTimeZoneActionListener(FileWriter writer, String dateFormat, ConverterParameters converterParameters) {
    this(writer, dateFormat, converterParameters.getTimeFormat(), converterParameters.getScale(), converterParameters.getStart(), converterParameters.getEnd(), converterParameters.getTimeFilter());
    this.converterParameters = converterParameters;
  }
}
