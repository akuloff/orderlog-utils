package com.monkeyquant.qsh;

import com.monkeyquant.jte.primitives.history.BarsCollector;
import com.monkeyquant.jte.primitives.history.BarsSaver;
import com.monkeyquant.jte.primitives.history.HistoryTick;
import com.monkeyquant.jte.primitives.history.PriceRecord;
import com.monkeyquant.jte.primitives.interfaces.IBarData;
import com.monkeyquant.jte.primitives.interfaces.IBookState;
import com.monkeyquant.jte.primitives.interfaces.ITickData;
import com.monkeyquant.jte.primitives.model.TradePeriod;
import com.monkeyquant.qsh.model.BookStateEvent;
import com.monkeyquant.qsh.model.TickDataEvent;
import lombok.extern.log4j.Log4j;

import java.io.FileWriter;
import java.sql.Timestamp;

@Log4j
public class BarsCollectorActionListener extends MoscowTimeZoneActionListener {
  private final boolean useMql;
  private final BarsSaver barsSaver;
  private final boolean useBookState;

  private double lastAsk = 0, lastBid = 0;

  public BarsCollectorActionListener(FileWriter writer, String dateFormat, boolean useMql, TradePeriod period, boolean useBookState, int scale, int startTime, int endTime) {
    super(writer, dateFormat, scale, startTime, endTime);
    this.useMql = useMql;
    this.useBookState = useBookState;
    this.barsSaver = new BarsSaver(new BarsCollector(period));
    this.barsSaver.setFillGaps(!useBookState);
  }

  private void processTickData(Timestamp tickTime, ITickData tickData, int spread) throws Exception {
      boolean isBarAdded = false;

      if (checkTime(tickData.getDate())) {
        isBarAdded = barsSaver.addNewTick(tickData);
      }

      if (isBarAdded) {
        IBarData barData = barsSaver.getCollector().getBarsList().get(barsSaver.getCollector().getBarsList().size() - 1);

        if (useMql) {
          writer.write(String.format("%s;%s;%s;%s;%s;%s;%s;%s;%s\n",
            mqlDateFormat.format(barData.getOpenDate()),
            mqlTimeFormat.format(barData.getOpenDate()),
            summFormat(barData.getOpenPrice()),   //open
            summFormat(barData.getHighPrice()),   //high
            summFormat(barData.getLowPrice()),    //low
            summFormat(barData.getClosePrice()),  //close
            barData.getValue(),     //tickvol
            barData.getValue(),      //vol
            spread
          ));
        } else {
          writer.write(String.format("%s;%s;%s;%s;%s;%s;%s\n",
            tickData.getInstrument().getCode(),
            formatter.format(barData.getOpenDate()),
            summFormat(barData.getOpenPrice()),   //open
            summFormat(barData.getHighPrice()),   //high
            summFormat(barData.getLowPrice()),    //low
            summFormat(barData.getClosePrice()),  //close
            barData.getValue()     //vol
          ));
        }
      }
  }


  @Override
  public void onBookChange(BookStateEvent bookStateEvent) throws Exception {
    if (useBookState) {
      Timestamp onTime = bookStateEvent.getTime();
      IBookState bookState = bookStateEvent.getBookState();
      PriceRecord bidRecord = bookState.getBeskBid();
      if (bidRecord != null) {
        double curBid = bidRecord.getPrice();
        if (curBid != lastBid) {
          lastBid = curBid;
          ITickData tickData = new HistoryTick(bookState.getInstrument(), onTime, lastBid, 1, "1", false, "", true);
          processTickData(onTime, tickData, 1);
        }
      }
    }
  }

  @Override
  public void onNewTick(TickDataEvent tickDataEvent) throws Exception{
    if (!useBookState) {
      processTickData(tickDataEvent.getTime(), tickDataEvent.getTickData(), 1);
    }
  }
}
