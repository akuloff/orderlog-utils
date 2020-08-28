package com.monkeyquant.qsh.listeners;

import com.monkeyquant.jte.primitives.interfaces.ITickData;
import com.monkeyquant.qsh.application.ConverterParameters;
import com.monkeyquant.qsh.model.CheckTimeResult;
import com.monkeyquant.qsh.model.IDataWriter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.util.Date;

@Slf4j
public abstract class MoscowTimeZoneListenerWithTimeQuant extends MoscowTimeZoneActionListener {
  protected final Integer timeQuantMsec;
  protected long lastQuant = 0;
  protected QuantTimeTicksCollector ticksCollector = null;

  //TODO migrate to Instant
  private Timestamp lastEventTime = null;

  @Override
  protected boolean checkTime(Date date) {
    if (lastEventTime != null && date.getTime() < lastEventTime.getTime()) {
      log.warn("data event is before lastEventDate, event: {},  lastEventDate: {}", new Timestamp(date.getTime()),  lastEventTime);
      return false;
    }
    lastEventTime = new Timestamp(date.getTime());
    return super.checkTime(date);
  }

  @Override
  protected String defaultDateFormat() {
    return "yyyy.MM.dd HH:mm:ss.SSS";
  }

  protected CheckTimeResult checkTimeQuant(Timestamp eventTime, ITickData tickData) {
    boolean doWrite = true;
    Date quantDate = new Date(eventTime.getTime());
    if (timeQuantMsec > 0) {
      long currentQuant = (eventTime.getTime() / timeQuantMsec) * timeQuantMsec;
      if (currentQuant > lastQuant) {
        lastQuant = currentQuant;
        quantDate = new Date(currentQuant);
        doWrite = ticksCollector != null;
      } else {
        if (tickData != null) {
          if (ticksCollector == null) {
            ticksCollector = new QuantTimeTicksCollector(tickData);
          } else {
            ticksCollector.addTick(tickData);
          }
        }
        doWrite = false;
      }
    }
    return CheckTimeResult.builder()
      .doWrite(doWrite)
      .quantDate(quantDate)
      .build();
  }


  public MoscowTimeZoneListenerWithTimeQuant(IDataWriter writer, ConverterParameters converterParameters) {
    super(writer, converterParameters);
    this.timeQuantMsec = converterParameters.getTimeQuant() != null ? converterParameters.getTimeQuant() : 0;
  }


}
