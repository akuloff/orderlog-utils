package com.monkeyquant.qsh.listeners;

import com.monkeyquant.jte.primitives.interfaces.IBookState;
import com.monkeyquant.jte.primitives.model.PriceRecord;
import com.monkeyquant.qsh.application.ConverterParameters;
import com.monkeyquant.qsh.model.BookStateEvent;
import com.monkeyquant.qsh.model.IDataWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.util.Date;
import java.util.List;

@Slf4j
public class BookStateWriterActionListener extends MoscowTimeZoneListenerWithTimeQuant {
  private final boolean mqlTick;
  private PriceRecord lastAsk = PriceRecord.builder().price(0d).value(0).build();
  private PriceRecord lastBid = PriceRecord.builder().price(0d).value(0).build();
  private final boolean writeZero;
  private long totalEvents = 0;
  private final int bookSize;

  private String formatTemplate;

  public BookStateWriterActionListener(IDataWriter writer, ConverterParameters converterParameters) {
    super(writer, converterParameters);
    this.mqlTick = converterParameters.getUseMql();
    this.writeZero = converterParameters.getWriteZero();
    this.bookSize = converterParameters.getBookSize();
    if (bookSize > 0 && mqlTick) {
      throw new IllegalArgumentException("MQL format incopatible with book_size > 0");
    }
  }

  @Override
  public void init() throws Exception {
    if (!converterParameters.getNoHeader()) {
      if (this.mqlTick) {
        writer.write("<DATE>;<TIME>;<BID>;<ASK>;<LAST>;<VOLUME>\n"); //format
        formatTemplate = "%s;%s;%s;%s;%s;1\n";
      } else {
        if (bookSize > 0) {
          StringBuilder sb = new StringBuilder("symbol;time;");
          StringBuilder sbf = new StringBuilder("%s;%s;");
          for (int i = 1; i < bookSize + 1; i++) {
            sb.append(String.format("ask%s;askvol%s;bid%s;bidvol%s;", i, i, i, i));
            sbf.append("%s;%s;%s;%s;");
          }
          String header = sb.toString();
          formatTemplate = sbf.toString();
          header = header.substring(0, header.length() - 1);
          formatTemplate = formatTemplate.substring(0, formatTemplate.length() - 1) + "\n";
          writer.write(header + "\n");
        } else {
          writer.write("symbol;time;ask;askvol;bid;bidvol\n"); //format
          formatTemplate = "%s;%s;%s;%s;%s;%s\n";
        }
      }
    }
  }

  @Override
  public void end() throws Exception {
    log.info("total events: {}", totalEvents);
  }


  @Override
  public void onBookChange(BookStateEvent bookStateEvent) {
    totalEvents ++;
    try {
      //writer.write(String.format("%s;%s;%s;%s\n", dateFormat.format(tickData.getTime()), tickData.getPrice(), tickData.getVolume(), tickData.getDealId()));
      IBookState bookState = bookStateEvent.getBookState();

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

      List<PriceRecord> askPrices;
      List<PriceRecord> bidPrices;


      if (doWrite) {
        if (bookSize > 0) {
          askPrices = bookState.getAskPositions(bookSize);
          bidPrices = bookState.getBidPositions(bookSize);
          if (askPrices.size() >= bookSize && bidPrices.size() >= bookSize) {
            PriceRecord firstAsk = askPrices.get(0);
            PriceRecord firstBid = bidPrices.get(0);
            if ((!firstAsk.equals(lastAsk) || !firstBid.equals(lastBid)) || timeQuantMsec > 0) {
              String instrCode = !StringUtils.isEmpty(this.converterParameters.getInstrCode()) ? this.converterParameters.getInstrCode() : bookState.getInstrument().getCode();
              StringBuilder outsb = new StringBuilder(String.format("%s;%s;", instrCode, dateFormat.format(eventDate)));
              for (int i = 0; i < bookSize; i++) {
                PriceRecord ask = askPrices.get(i);
                PriceRecord bid = bidPrices.get(i);
                outsb.append(String.format("%s;%s;%s;%s", summFormat(ask.getPrice()), ask.getValue(), summFormat(bid.getPrice()), bid.getValue()));
                if (i < bookSize - 1) {
                  outsb.append(";");
                }
              }
              writer.write(outsb.append("\n").toString());
              lastAsk = firstAsk;
              lastBid = firstBid;
            }
          }
        } else {
          askPrices = bookState.getAskPositionsForVolume(1);
          bidPrices = bookState.getBidPositionsForVolume(1);

          if ((askPrices.size() > 0 && bidPrices.size() > 0) || writeZero) {
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

            if ((!ask.equals(lastAsk) || !bid.equals(lastBid)) || timeQuantMsec > 0) {
              String outs;
              if (mqlTick) {
                outs = String.format(formatTemplate, mqlDateFormat.format(eventDate), mqlTimeFormat.format(eventDate), summFormat(bid.getPrice()), summFormat(ask.getPrice()), summFormat(ask.getPrice()));
              } else {
                String instrCode = !StringUtils.isEmpty(this.converterParameters.getInstrCode()) ? this.converterParameters.getInstrCode() : bookState.getInstrument().getCode();
                outs = String.format(formatTemplate, instrCode, dateFormat.format(eventDate), summFormat(ask.getPrice()), ask.getValue(), summFormat(bid.getPrice()), bid.getValue());
              }
              writer.write(outs);
              lastAsk = ask;
              lastBid = bid;
            }
          }
        }
      }
    } catch (Exception e) {
      log.error("write exception", e);
    }
  }
}
