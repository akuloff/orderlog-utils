package com.monkeyquant.qsh.listeners;

import com.monkeyquant.qsh.application.ConverterParameters;
import com.monkeyquant.qsh.model.IDataWriter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MoscowTimeZoneListenerWithTimeQuant extends MoscowTimeZoneActionListener {
  protected final Integer timeQuantMsec;
  protected long lastQuant = 0;

  public MoscowTimeZoneListenerWithTimeQuant(IDataWriter writer, ConverterParameters converterParameters) {
    super(writer, converterParameters);
    this.timeQuantMsec = converterParameters.getTimeQuant() != null ? converterParameters.getTimeQuant() : 0;
  }
}
