package com.monkeyquant.qsh.listeners;

import com.monkeyquant.qsh.application.ConverterParameters;
import com.monkeyquant.qsh.model.IDataWriter;
import com.monkeyquant.qsh.model.IMarketActionListener;
import com.monkeyquant.qsh.model.TimeFilter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

@Slf4j
public abstract class MoscowTimeZoneActionListener implements IMarketActionListener {
  protected final IDataWriter writer;
  protected final SimpleDateFormat dateFormat;
  protected final SimpleDateFormat timeFormat;
  protected final Calendar calendar;
  protected final SimpleDateFormat mqlDateFormat;
  protected final SimpleDateFormat mqlTimeFormat;
  protected int startTime;
  protected int endTime;
  protected int scale;
  protected final TimeFilter timeFilter;
  protected final ConverterParameters converterParameters;

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

  protected abstract String defaultDateFormat();

  public MoscowTimeZoneActionListener(IDataWriter writer, ConverterParameters converterParameters) {
    this.calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT+3"));
    this.converterParameters = converterParameters;
    this.writer = writer;
    this.dateFormat = new SimpleDateFormat(StringUtils.isEmpty(converterParameters.getDateFormat()) ? defaultDateFormat() : converterParameters.getDateFormat());
    this.dateFormat.setCalendar(calendar);
    this.dateFormat.setTimeZone(calendar.getTimeZone());

    if (!StringUtils.isEmpty(converterParameters.getTimeFormat())) {
      this.timeFormat = new SimpleDateFormat(converterParameters.getTimeFormat());
      this.timeFormat.setCalendar(calendar);
      this.timeFormat.setTimeZone(calendar.getTimeZone());
    } else {
      this.timeFormat = null;
    }

    mqlDateFormat = new SimpleDateFormat("yyyy.MM.dd");
    mqlDateFormat.setCalendar(calendar);
    mqlDateFormat.setTimeZone(calendar.getTimeZone());
    mqlTimeFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    mqlTimeFormat.setCalendar(calendar);
    mqlTimeFormat.setTimeZone(calendar.getTimeZone());

    this.startTime = converterParameters.getStart();
    this.endTime = converterParameters.getEnd();
    this.scale = converterParameters.getScale();
    this.timeFilter = converterParameters.getTimeFilter();
  }

}
