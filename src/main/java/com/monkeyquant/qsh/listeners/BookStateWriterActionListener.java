package com.monkeyquant.qsh.listeners;

import com.monkeyquant.jte.primitives.interfaces.IBookState;
import com.monkeyquant.jte.primitives.model.PriceRecord;
import com.monkeyquant.qsh.application.ConverterParameters;
import com.monkeyquant.qsh.model.BookStateEvent;
import com.monkeyquant.qsh.model.IDataWriter;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;
import java.util.List;

@Slf4j
public class BookStateWriterActionListener extends MoscowTimeZoneListenerWithTimeQuant {
  private final boolean mqlTick;
  private PriceRecord lastAsk = PriceRecord.builder().price(0d).value(0).build();
  private PriceRecord lastBid = PriceRecord.builder().price(0d).value(0).build();
  private final boolean writeZero;

  public BookStateWriterActionListener(IDataWriter writer, ConverterParameters converterParameters) {
    super(writer, converterParameters);
    this.mqlTick = converterParameters.getUseMql();
    this.writeZero = converterParameters.getWriteZero();
  }

  @Override
  public void init() throws Exception {
    if (!converterParameters.getNoHeader()) {
      if (converterParameters.getUseMql()) {
        writer.write("<DATE>;<TIME>;<BID>;<ASK>;<LAST>;<VOLUME>\n"); //format
      } else {
        writer.write("symbol;time;ask;askvol;bid;bidvol\n"); //format
      }
    }
  }

  @Override
  protected String defaultDateFormat() {
    return "yyyy.MM.dd HH:mm:ss.SSS";
  }

  @Override
  public void onBookChange(BookStateEvent bookStateEvent) {
    try {
      //writer.write(String.format("%s;%s;%s;%s\n", dateFormat.format(tickData.getTime()), tickData.getPrice(), tickData.getVolume(), tickData.getDealId()));
      IBookState bookState = bookStateEvent.getBookState();
      List<PriceRecord> askPrices = bookState.getAskPositionsForVolume(1);
      List<PriceRecord> bidPrices = bookState.getBidPositionsForVolume(1);
      if ((askPrices.size() > 0 && bidPrices.size() > 0) || writeZero) {

        Date eventDate = bookStateEvent.getTime();
        boolean doWrite = checkTime(eventDate);

        if (doWrite && timeQuantMsec > 0) {
          long currentQuant = (eventDate.getTime() / timeQuantMsec) * timeQuantMsec;
          doWrite = currentQuant > lastQuant;
          if (doWrite) {
            lastQuant = currentQuant;
            eventDate = new Date(currentQuant);
          }
        }

        if (doWrite) {
          PriceRecord ask, bid;
          if (askPrices.size() > 0) {
            ask = askPrices.get(0);
          } else {
            ask = PriceRecord.builder().price(0d).value(1).build();
          }
          if (bidPrices.size() > 0) {
            bid = bidPrices.get(0);
          } else {
            bid = PriceRecord.builder().price(0d).value(1).build();
          }

          if (!ask.equals(lastAsk) || !bid.equals(lastBid)) {
            String outs;
            if (mqlTick) {
              outs = String.format("%s;%s;%s;%s;%s;1\n", mqlDateFormat.format(eventDate), mqlTimeFormat.format(eventDate), summFormat(bid.getPrice()), summFormat(ask.getPrice()), summFormat(ask.getPrice()));
            } else {
              outs = String.format("%s;%s;%s;%s;%s;%s\n", bookState.getInstrument().getCode(), dateFormat.format(eventDate), summFormat(ask.getPrice()), ask.getValue(), summFormat(bid.getPrice()), bid.getValue());
            }
            //outs = "symbol;time;ask;bid;askvol;bidvol\n";
            writer.write(outs);
            lastAsk = ask;
            lastBid = bid;
          }
        }
      }
    } catch (Exception e) {
      log.error("write exception", e);
    }
  }
}
