package com.monkeyquant.qsh.listeners;

import com.monkeyquant.jte.primitives.interfaces.ITickData;
import com.monkeyquant.qsh.application.ConverterParameters;
import com.monkeyquant.qsh.model.IDataWriter;
import com.monkeyquant.qsh.model.TickDataEvent;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.util.Date;

@Slf4j
public class TicksWriterActionListener extends MoscowTimeZoneListenerWithTimeQuant {
  //TODO separate classes for mql and not
  private final boolean useMql;
  private final boolean saveTradeId;
  private QuantTimeTicksCollector ticksCollector = null;

  public TicksWriterActionListener(IDataWriter writer, ConverterParameters converterParameters) {
    super(writer, converterParameters);
    this.useMql = converterParameters.getUseMql();
    this.saveTradeId = converterParameters.getSaveTradeId();
    if (useMql && timeQuantMsec > 0) {
      throw new IllegalArgumentException("time quant not compatible with MQL format");
    }
  }

  @Override
  public void init() throws Exception {
    if (!converterParameters.getNoHeader()) {
      if (useMql) {
        writer.write("<DATE>;<TIME>;<BID>;<ASK>;<LAST>;<VOLUME>\n"); //format
      } else {
        if (timeQuantMsec > 0) {
          writer.write("symbol;time;count;volume;open;high;low;close;avg\n");
        } else {
          if (saveTradeId) {
            writer.write("symbol;time;price;volume;deal_id\n");
          } else {
            writer.write("symbol;time;price;volume\n");
          }
        }
      }
    }
  }

  @Override
  protected String defaultDateFormat() {
    return "yyyy.MM.dd HH:mm:ss.SSS";
  }

  @Override
  public void onNewTick(TickDataEvent tickDataEvent) {
    try {
      ITickData tickData = tickDataEvent.getTickData();
      Timestamp tickTime = tickDataEvent.getTime();
      if (checkTime(tickTime)) {
        if (useMql) {
          writer.write(String.format("%s;%s;%s;%s;%s;%s\n",
            mqlDateFormat.format(tickTime), mqlTimeFormat.format(tickTime),
            summFormat(tickData.getPrice()), summFormat(tickData.getPrice()), summFormat(tickData.getPrice()), tickData.getAmount()));
        } else {
          boolean doWrite = true;
          Date tickDate = new Date(tickTime.getTime());
          if (timeQuantMsec > 0) {
            long currentQuant = (tickTime.getTime() / timeQuantMsec) * timeQuantMsec;
            if (currentQuant > lastQuant) {
              lastQuant = currentQuant;
              tickDate = new Date(currentQuant);
              doWrite = ticksCollector != null;
            } else {
              if (ticksCollector == null) {
                ticksCollector = new QuantTimeTicksCollector(tickData);
              } else {
                ticksCollector.addTick(tickData);
              }
              doWrite = false;
            }
          }
          if (doWrite) {
            String outString = "";
            if (timeQuantMsec > 0 ) {
              //writer.write("symbol;time;count;volume;open;high;low;close;avg\n");
              outString = String.format("%s;%s;%s;%s;%s;%s;%s;%s;%s", tickData.getInstrument().getCode(), dateFormat.format(tickDate),
                ticksCollector.getTotalTicks(), ticksCollector.getTotalVolume(),
                //open;high;low;close;avg
                summFormat(ticksCollector.getOpenPrice()),
                summFormat(ticksCollector.getHighPrice()),
                summFormat(ticksCollector.getLowPrice()),
                summFormat(ticksCollector.getClosePrice()),
                summFormat(ticksCollector.getAvgPrice())
              );
              ticksCollector = new QuantTimeTicksCollector(tickData);
            } else {
              outString = String.format("%s;%s;%s;%s", tickData.getInstrument().getCode(), dateFormat.format(tickDate), summFormat(tickData.getPrice()), tickData.getAmount());
              if (converterParameters.getSaveTradeId()) {
                outString = String.format("%s;%s", outString, tickData.getTradeId());
              }
            }
            writer.write(String.format("%s\n", outString));
          }

        }
      }
    } catch (Exception e) {
      log.error("write exception", e);
    }
  }
}
