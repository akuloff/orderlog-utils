package com.monkeyquant.qsh.listeners;

import com.monkeyquant.jte.primitives.history.BarsCollector;
import com.monkeyquant.jte.primitives.history.BarsSaver;
import com.monkeyquant.jte.primitives.history.HistoryTick;
import com.monkeyquant.jte.primitives.interfaces.IBarData;
import com.monkeyquant.jte.primitives.interfaces.IBookState;
import com.monkeyquant.jte.primitives.interfaces.ITickData;
import com.monkeyquant.jte.primitives.model.PriceRecord;
import com.monkeyquant.jte.primitives.model.TradePeriod;
import com.monkeyquant.qsh.application.ConverterParameters;
import com.monkeyquant.qsh.model.BookStateEvent;
import com.monkeyquant.qsh.model.IDataWriter;
import com.monkeyquant.qsh.model.TickDataEvent;
import com.monkeyquant.qsh.model.TimeOfBar;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.util.Date;

@Slf4j
public class BarsCollectorActionListener extends MoscowTimeZoneActionListener {
  private final boolean useMql;
  private final BarsSaver barsSaver;
  private final boolean useBookState;
  private final TimeOfBar timeOfBar;

  private double lastAsk = 0, lastBid = 0;
  private Timestamp lastBookTime = null;
  private final TradePeriod period;

  private final boolean closeOnly;

  @Override
  public void init() throws Exception {
    if (!converterParameters.getNoHeader()) {
      if (converterParameters.getUseMql()) {
        writer.write("<DATE>;<TIME>;<OPEN>;<HIGH>;<LOW>;<CLOSE>;<TICKVOL>;<VOL>;<SPREAD>\n"); //format
      } else {
        if (StringUtils.isEmpty(converterParameters.getTimeFormat())) {
          writer.write("<TICKER>;<DATE>;<OPEN>;<HIGH>;<LOW>;<CLOSE>;<VOL>\n"); //format
        } else {
          writer.write("<TICKER>;<DATE>;<TIME>;<OPEN>;<HIGH>;<LOW>;<CLOSE>;<VOL>\n"); //format
        }
      }
    }
  }

  @Override
  protected String defaultDateFormat() {
    return "yyyy.MM.dd HH:mm";
  }

  public BarsCollectorActionListener(IDataWriter writer, ConverterParameters converterParameters) {
    super(writer, converterParameters);
    this.useMql = converterParameters.getUseMql();
    this.useBookState = converterParameters.getUseBookState();
    this.period = TradePeriod.fromString(converterParameters.getBarPeriod().toString());
    this.barsSaver = new BarsSaver(new BarsCollector(this.period));
    this.barsSaver.setFillGaps(!useBookState);
    this.timeOfBar = converterParameters.getBarTime();
    this.closeOnly = converterParameters.getCloseOnly();
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

  private void processTickData(ITickData tickData, int spread) throws Exception {
      boolean isBarAdded = false;

      if (checkTime(tickData.getDate())) {
        isBarAdded = barsSaver.addNewTick(tickData);
      }

      if (isBarAdded) {
        IBarData barData = barsSaver.getCollector().getBarsList().get(barsSaver.getCollector().getBarsList().size() - 1);

        Date barDate;
        if (timeOfBar.equals(TimeOfBar.CLOSE)) {
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
          String instrCode = !org.springframework.util.StringUtils.isEmpty(this.converterParameters.getInstrCode()) ? this.converterParameters.getInstrCode() : tickData.getInstrument().getCode();
          if (timeFormat == null) {
            writer.write(String.format("%s;%s;%s;%s\n",
                    instrCode,
                    dateFormat.format(barDate),
                    getPrices(barData, closeOnly),
                    barData.getValue()     //vol
            ));
          } else {
            writer.write(String.format("%s;%s;%s;%s;%s\n",
                    instrCode,
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
      } else if (onTime.getTime() < lastBookTime.getTime()){
        log.warn("BookStateEvent data is before lastBookTime, event: {},  lastEventDate: {}", onTime, lastBookTime);
      }
      if (bidRecord != null) {
        double curBid = bidRecord.getPrice();
        if (curBid != lastBid || (onTime.getTime() - lastBookTime.getTime() > period.getPeriodMsec())) {
          lastBid = curBid;
          ITickData tickData = new HistoryTick(bookState.getInstrument(), onTime, lastBid, 1, "1", false, "", true);
          processTickData(tickData, 1);
          lastBookTime = onTime;
        }
      }
    }
  }

  @Override
  public void onNewTick(TickDataEvent tickDataEvent) throws Exception{
    if (!useBookState) {
      processTickData(tickDataEvent.getTickData(), 1);
    }
  }
}
