package com.monkeyquant.qsh;

import com.monkeyquant.jte.primitives.history.PriceRecord;
import com.monkeyquant.jte.primitives.interfaces.IBookState;
import com.monkeyquant.qsh.model.BookStateEvent;
import lombok.extern.log4j.Log4j;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Log4j
public class BookStateWriterActionListener extends MoscowTimeZoneActionListener {
  private final Integer timeQuantMsec;
  private final boolean mqlTick;
  private PriceRecord lastAsk = PriceRecord.builder().price(0d).value(0).build();
  private PriceRecord lastBid = PriceRecord.builder().price(0d).value(0).build();
  private SimpleDateFormat mqlDateFormat = null;
  private SimpleDateFormat mqlTimeFormat = null;

  private long lastQuant = 0;

  public BookStateWriterActionListener(FileWriter writer, String dateFormat, Integer timeQuant, boolean mqlTick) {
    super(writer, dateFormat);
    this.timeQuantMsec = timeQuant != null ? timeQuant : 0;
    this.mqlTick = mqlTick;
    if (mqlTick) {
      mqlDateFormat = new SimpleDateFormat("yyyy.MM.dd");
      mqlDateFormat.setCalendar(calendar);
      mqlDateFormat.setTimeZone(calendar.getTimeZone());
      mqlTimeFormat = new SimpleDateFormat("HH:mm:ss.SSS");
      mqlTimeFormat.setCalendar(calendar);
      mqlTimeFormat.setTimeZone(calendar.getTimeZone());
    }
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

        boolean doWrite = true;
        Date eventDate = bookStateEvent.getTime();
        if (timeQuantMsec > 0) {
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
            outs = String.format("%s;%s;%s;%s;0;0\n", mqlDateFormat.format(eventDate), mqlTimeFormat.format(eventDate), bid.getPrice(), ask.getPrice());
          } else {
            outs = String.format("%s;%s;%s;%s;%s;%s\n", bookState.getInstrument().getCode(), formatter.format(eventDate), ask.getPrice(), ask.getValue(), bid.getPrice(), bid.getValue());
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
