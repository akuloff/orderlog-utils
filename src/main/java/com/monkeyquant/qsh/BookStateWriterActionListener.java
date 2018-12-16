package com.monkeyquant.qsh;

import com.monkeyquant.jte.primitives.history.PriceRecord;
import com.monkeyquant.jte.primitives.interfaces.IBookState;
import com.monkeyquant.qsh.model.BookStateEvent;
import lombok.extern.log4j.Log4j;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.List;

@Log4j
public class BookStateWriterActionListener extends MoscowTimeZoneActionListener {
  private final Integer timeQuantMsec;
  private final boolean mqlTick;
  private PriceRecord lastAsk = PriceRecord.builder().price(0d).value(0).build();
  private PriceRecord lastBid = PriceRecord.builder().price(0d).value(0).build();

  private long lastQuant = 0;

  public BookStateWriterActionListener(FileWriter writer, String dateFormat, Integer timeQuant, boolean mqlTick, int scale, int startTime, int endTime) {
    super(writer, dateFormat, scale, startTime, endTime);
    this.timeQuantMsec = timeQuant != null ? timeQuant : 0;
    this.mqlTick = mqlTick;
  }

  @Override
  public void onBookChange(BookStateEvent bookStateEvent) {
    try {
      //writer.write(String.format("%s;%s;%s;%s\n", formatter.format(tickData.getTime()), tickData.getPrice(), tickData.getVolume(), tickData.getDealId()));
      IBookState bookState = bookStateEvent.getBookState();
      List<PriceRecord> askPrices = bookState.getAskPositionsForVolume(1);
      List<PriceRecord> bidPrices = bookState.getBidPositionsForVolume(1);
      if (askPrices.size() > 0 && bidPrices.size() > 0) {
        PriceRecord ask = askPrices.get(0);
        PriceRecord bid = bidPrices.get(0);

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

        if (doWrite && (!ask.equals(lastAsk) || !bid.equals(lastBid))) {
          String outs;
          if (mqlTick) {
            outs = String.format("%s;%s;%s;%s;%s;1\n", mqlDateFormat.format(eventDate), mqlTimeFormat.format(eventDate), summFormat(bid.getPrice()), summFormat(ask.getPrice()), summFormat(ask.getPrice()));
          } else {
            outs = String.format("%s;%s;%s;%s;%s;%s\n", bookState.getInstrument().getCode(), formatter.format(eventDate), summFormat(ask.getPrice()), ask.getValue(), summFormat(bid.getPrice()), bid.getValue());
          }
          //outs = "symbol;time;ask;bid;askvol;bidvol\n";
          writer.write(outs);
          lastAsk = ask;
          lastBid = bid;
        }
      }
    } catch (IOException e) {
      log.error("write exception", e);
    }
  }
}
