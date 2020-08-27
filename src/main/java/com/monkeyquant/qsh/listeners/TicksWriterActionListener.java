package com.monkeyquant.qsh.listeners;

import com.monkeyquant.jte.primitives.interfaces.ITickData;
import com.monkeyquant.qsh.application.ConverterParameters;
import com.monkeyquant.qsh.model.CheckTimeResult;
import com.monkeyquant.qsh.model.IDataWriter;
import com.monkeyquant.qsh.model.TickDataEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.sql.Timestamp;
import java.util.Date;

@Slf4j
public class TicksWriterActionListener extends MoscowTimeZoneListenerWithTimeQuant {
  //TODO separate classes for mql and not
  private final boolean useMql;
  private final boolean saveTradeId;

  //TODO migrate to Instant
  private Timestamp lastEventTime = null;

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
  protected boolean checkTime(Date date) {
    if (lastEventTime != null && date.getTime() < lastEventTime.getTime()) {
      log.warn("tick data event is before lastEventDate, event: {},  lastEventDate: {}", new Timestamp(date.getTime()),  lastEventTime);
      return false;
    }
    lastEventTime = new Timestamp(date.getTime());
    return super.checkTime(date);
  }

  @Override
  public void onNewTick(TickDataEvent tickDataEvent) {
    try {
      ITickData tickData = tickDataEvent.getTickData();
      Timestamp tickTime = Timestamp.from(tickDataEvent.getTime());
      if (this.checkTime(tickTime)) {
        if (useMql) {
          writer.write(String.format("%s;%s;%s;%s;%s;%s\n",
            mqlDateFormat.format(tickTime), mqlTimeFormat.format(tickTime),
            summFormat(tickData.getPrice()), summFormat(tickData.getPrice()), summFormat(tickData.getPrice()), tickData.getAmount()));
        } else {
          CheckTimeResult checkTimeResult = checkTimeQuant(tickTime, tickData);
          boolean doWrite = checkTimeResult.isDoWrite();
          Date tickDate = checkTimeResult.getQuantDate();
          if (doWrite) {
            String outString = "";
            String instrCode = !StringUtils.isEmpty(this.converterParameters.getInstrCode()) ? this.converterParameters.getInstrCode() : tickData.getInstrument().getCode();

            if (timeQuantMsec > 0 ) {
              //writer.write("symbol;time;count;volume;open;high;low;close;avg\n");
              outString = String.format("%s;%s;%s;%s;%s;%s;%s;%s;%s", instrCode, dateFormat.format(tickDate),
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
              outString = String.format("%s;%s;%s;%s", instrCode, dateFormat.format(tickDate), summFormat(tickData.getPrice()), tickData.getAmount());
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
