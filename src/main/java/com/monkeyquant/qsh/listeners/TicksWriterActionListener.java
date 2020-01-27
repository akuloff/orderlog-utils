package com.monkeyquant.qsh.listeners;

import com.monkeyquant.jte.primitives.interfaces.ITickData;
import com.monkeyquant.qsh.application.ConverterParameters;
import com.monkeyquant.qsh.model.TickDataEvent;
import lombok.extern.log4j.Log4j;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;

@Log4j
public class TicksWriterActionListener extends MoscowTimeZoneActionListener {
  //TODO separate classes for mql and not
  private final boolean useMql;
  private final boolean saveTradeId;

  public TicksWriterActionListener(FileWriter writer, String dateFormat, ConverterParameters converterParameters) {
    super(writer, dateFormat, converterParameters);
    this.useMql = converterParameters.getUseMql();
    this.saveTradeId = converterParameters.getSaveTradeId();
  }

  @Override
  public void init() throws Exception {
    if (!converterParameters.getNoHeader()) {
      if (converterParameters.getUseMql()) {
        writer.write("<DATE>;<TIME>;<BID>;<ASK>;<LAST>;<VOLUME>\n"); //format
      } else {
        if (converterParameters.getSaveTradeId()) {
          writer.write("symbol;time;price;volume;deal_id\n");
        } else {
          writer.write("symbol;time;price;volume\n");
        }
      }
    }
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
          String mainString = String.format("%s;%s;%s;%s", tickData.getInstrument().getCode(), dateFormat.format(tickTime), summFormat(tickData.getPrice()), tickData.getAmount());
          if (converterParameters.getSaveTradeId()) {
            writer.write(String.format("%s;%s\n", mainString, tickData.getTradeId()));
          } else {
            writer.write(String.format("%s\n", mainString));
          }
        }
      }
    } catch (IOException e) {
      log.error("write exception", e);
    }
  }
}
