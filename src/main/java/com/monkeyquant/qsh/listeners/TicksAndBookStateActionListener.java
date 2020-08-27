package com.monkeyquant.qsh.listeners;

import com.monkeyquant.jte.primitives.interfaces.IBookState;
import com.monkeyquant.jte.primitives.interfaces.ITickData;
import com.monkeyquant.jte.primitives.model.PriceRecord;
import com.monkeyquant.qsh.application.ConverterParameters;
import com.monkeyquant.qsh.model.BookStateEvent;
import com.monkeyquant.qsh.model.CheckTimeResult;
import com.monkeyquant.qsh.model.IDataWriter;
import com.monkeyquant.qsh.model.TickDataEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.sql.Timestamp;
import java.util.Date;

@Slf4j
public class TicksAndBookStateActionListener extends MoscowTimeZoneListenerWithTimeQuant {
  private PriceRecord lastAsk = PriceRecord.builder().price(0d).value(0).build();
  private PriceRecord lastBid = PriceRecord.builder().price(0d).value(0).build();
  private final boolean saveTradeId;

  public TicksAndBookStateActionListener(IDataWriter writer, ConverterParameters converterParameters) {
    super(writer, converterParameters);
    this.saveTradeId = converterParameters.getSaveTradeId();
  }

  @Override
  public void init() throws Exception {
    if (!converterParameters.getNoHeader()) {
      String header = "symbol;time;ask;bid;last;volume";
      if (saveTradeId) {
        header = header + ";deal_id";
      }
      writer.write(header + "\n");
    }
  }

  @Override
  public void onBookChange(BookStateEvent bookStateEvent) throws Exception {
    IBookState bookState = bookStateEvent.getBookState();
    Date eventDate = bookStateEvent.getTime();
    boolean doWrite = checkTime(eventDate);
    if (doWrite) {
      CheckTimeResult checkTimeResult = checkTimeQuant(bookState.getDate(), null);
      doWrite = checkTimeResult.isDoWrite();
      eventDate = checkTimeResult.getQuantDate();
    }
    if (doWrite) {
      PriceRecord ask = bookState.getBestAsk();
      PriceRecord bid = bookState.getBestBid();
      if (ask != null && bid != null){
        if (ask.getPrice() != lastAsk.getPrice() || bid.getPrice() != lastBid.getPrice() || timeQuantMsec > 0) {
          String instrCode = !StringUtils.isEmpty(this.converterParameters.getInstrCode()) ? this.converterParameters.getInstrCode() : bookState.getInstrument().getCode();
          String outs = instrCode + ";" + dateFormat.format(eventDate) + ";" + summFormat(ask.getPrice()) + ";" + summFormat(bid.getPrice()) + ";;1";
          if (saveTradeId) {
            outs = outs + ";0";
          }
          outs = outs + "\n";
          try {
            writer.write(outs);
            lastAsk = ask;
            lastBid = bid;
          } catch (Exception e) {
            log.warn("book write exception", e);
          }
        }
      }
    }
  }

  @Override
  public void onNewTick(TickDataEvent tickDataEvent) throws Exception {
    ITickData tickData = tickDataEvent.getTickData();
    Timestamp tickTime = Timestamp.from(tickDataEvent.getTime());
    Date eventDate = tickData.getDate();
    boolean doWrite = checkTime(eventDate);
    if (doWrite) {
      CheckTimeResult checkTimeResult = checkTimeQuant(tickTime, tickData);
      doWrite = checkTimeResult.isDoWrite();
      eventDate = checkTimeResult.getQuantDate();
      if (doWrite) {
        String outs = "";
        String instrCode = !StringUtils.isEmpty(this.converterParameters.getInstrCode()) ? this.converterParameters.getInstrCode() : tickData.getInstrument().getCode();
        if (timeQuantMsec > 0 ) {
          ticksCollector = new QuantTimeTicksCollector(tickData);
        }
        outs = instrCode + ";" + dateFormat.format(eventDate);
        outs = outs + ";";
        if (lastAsk.getPrice() != 0) {
           outs = outs + summFormat(lastAsk.getPrice());
        }
        outs = outs + ";";
        if (lastBid.getPrice() != 0) {
          outs = outs + summFormat(lastBid.getPrice());
        }
        outs = outs +  ";" + summFormat(tickData.getPrice()) + ";" + tickData.getAmount();
        if (saveTradeId) {
          outs = outs + ";" + tickData.getTradeId();
        }
        outs = outs + "\n";
        try {
          writer.write(outs);
        } catch (Exception e) {
          log.warn("tick write exception", e);
        }
      }
    }
  }


}
