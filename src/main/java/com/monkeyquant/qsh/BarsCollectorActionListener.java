package com.monkeyquant.qsh;

import com.monkeyquant.jte.primitives.history.BarsCollector;
import com.monkeyquant.jte.primitives.history.BarsSaver;
import com.monkeyquant.jte.primitives.history.HistoryTick;
import com.monkeyquant.jte.primitives.history.PriceRecord;
import com.monkeyquant.jte.primitives.interfaces.IBarData;
import com.monkeyquant.jte.primitives.interfaces.IBookState;
import com.monkeyquant.jte.primitives.interfaces.ITickData;
import com.monkeyquant.jte.primitives.model.TradePeriod;
import com.monkeyquant.qsh.application.TimeFilter;
import com.monkeyquant.qsh.application.TimeOfBar;
import com.monkeyquant.qsh.model.BookStateEvent;
import com.monkeyquant.qsh.model.TickDataEvent;
import lombok.extern.log4j.Log4j;

import java.io.FileWriter;
import java.sql.Timestamp;
import java.util.Date;

@Log4j
public class BarsCollectorActionListener extends MoscowTimeZoneActionListener {
  private final boolean useMql;
  private final BarsSaver barsSaver;
  private final boolean useBookState;
  private final TimeOfBar timeOfBar;

  private double lastAsk = 0, lastBid = 0;
  private Timestamp lastBookTime = null;
  private final TradePeriod period;

  private final boolean closeOnly;

  public BarsCollectorActionListener(FileWriter writer, String dateFormat, String timeFormat, boolean useMql,
                                     TradePeriod period, boolean useBookState, int scale, int startTime, int endTime, TimeOfBar timeOfBar, TimeFilter timeFilter, boolean closeOnly) {
    super(writer, dateFormat, timeFormat, scale, startTime, endTime, timeFilter);
    this.useMql = useMql;
    this.useBookState = useBookState;
    this.barsSaver = new BarsSaver(new BarsCollector(period));
    this.barsSaver.setFillGaps(!useBookState);
    this.timeOfBar = timeOfBar;
    this.period = period;
    this.closeOnly = closeOnly;
  }

  private String getPrices(IBarData barData, boolean closeOnly){
    if (!closeOnly) {
      return String.format("%s;%s;%s;%s",
              summFormat(barData.getOpenPrice()),
              summFormat(barData.getHighPrice()),
              summFormat(barData.getLowPrice()),
              summFormat(barData.getClosePrice())
      );
    } else {
      return String.format("%s;%s;%s;%s",
              summFormat(barData.getClosePrice()),
              summFormat(barData.getClosePrice()),
              summFormat(barData.getClosePrice()),
              summFormat(barData.getClosePrice())
      );
    }
  }

  private void processTickData(Timestamp tickTime, ITickData tickData, int spread) throws Exception {
      boolean isBarAdded = false;

      if (checkTime(tickData.getDate())) {
        isBarAdded = barsSaver.addNewTick(tickData);
      }

      if (isBarAdded) {
        IBarData barData = barsSaver.getCollector().getBarsList().get(barsSaver.getCollector().getBarsList().size() - 1);

        Date barDate;
        if (timeOfBar.equals(TimeOfBar.close)) {
          barDate = barData.getCloseDate();
        } else {
          barDate = barData.getOpenDate();
        }

        if (useMql) {
          writer.write(String.format("%s;%s;%s;%s;%s;%s\n",
            mqlDateFormat.format(barDate),
            mqlTimeFormat.format(barDate),
            getPrices(barData, closeOnly),
            barData.getValue(),     //tickvol
            barData.getValue(),      //vol
            spread
          ));
        } else {
          if (timeFormat == null) {
            writer.write(String.format("%s;%s;%s;%s\n",
                    tickData.getInstrument().getCode(),
                    dateFormat.format(barDate),
                    getPrices(barData, closeOnly),
                    barData.getValue()     //vol
            ));
          } else {
            writer.write(String.format("%s;%s;%s;%s;%s\n",
                    tickData.getInstrument().getCode(),
                    dateFormat.format(barDate),
                    timeFormat.format(barDate),
                    getPrices(barData, closeOnly),
                    barData.getValue()     //vol
            ));
          }
        }
      }
  }


  @Override
  public void onBookChange(BookStateEvent bookStateEvent) throws Exception {
    if (useBookState) {
      Timestamp onTime = bookStateEvent.getTime();
      IBookState bookState = bookStateEvent.getBookState();
      PriceRecord bidRecord = bookState.getBestBid();
      if (lastBookTime == null) {
        lastBookTime = onTime;
      }
      if (bidRecord != null) {
        double curBid = bidRecord.getPrice();
        if (curBid != lastBid || (onTime.getTime() - lastBookTime.getTime() > period.getPeriodMsec())) {
          lastBid = curBid;
          ITickData tickData = new HistoryTick(bookState.getInstrument(), onTime, lastBid, 1, "1", false, "", true);
          processTickData(onTime, tickData, 1);
          lastBookTime = onTime;
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
