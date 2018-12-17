package com.monkeyquant.qsh.application;

import com.alex09x.qsh.reader.QshReaderFactory;
import com.alex09x.qsh.reader.type.OrdersLogRecord;
import com.monkeyquant.jte.primitives.model.TradePeriod;
import com.monkeyquant.qsh.*;
import com.monkeyquant.qsh.model.IOrdersProcessor;
import com.monkeyquant.qsh.model.OutputFileType;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

@Log4j
public class QshFilesConverterApplication {

  private static void processInputFile(IOrdersProcessor ordersProcessor, String inputFileName) throws IOException{
    QshReaderFactory qshReaderFactory = new QshReaderFactory();
    Iterator<OrdersLogRecord> ordersLogRecordIterator = qshReaderFactory.openPath(inputFileName);
    while ( ordersLogRecordIterator.hasNext() ) {
      OrdersLogRecord ordersLogRecord = ordersLogRecordIterator.next();
      ordersProcessor.processOrderRecord(ordersLogRecord);
    }
  }

  private static String initOutFile(ConverterParameters parameters){
    String outFileName = null, appender = "";

    String outfile = parameters.getOutputFile();
    if (!StringUtils.isEmpty(outfile)) {
      return outfile;
    }

    OutputFileType fileType = parameters.getOutputFileType();

    switch (fileType) {
      case TICKS:
        appender = "_ticks.csv";
        break;
      case BARS:
        if (parameters.getBarPeriod() != null) {
          appender = "_" + parameters.getBarPeriod().toString();
        }
        appender = appender + "_bars.csv";
        break;
      case BOOKSTATE:
        if (parameters.getTimeQuant() != null && parameters.getTimeQuant() > 0) {
          appender = "_t" + parameters.getTimeQuant();
        }
        appender = appender + "_book.csv";
        break;
      default:
        appender = ".csv";
    }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmm");
    outFileName = String.format("out_%s%s", sdf.format(new Date()), appender);

    return outFileName;
  }

  public static void main(String[] args) {
    FileWriter writer;
    String outFileName;

    ConverterParameters converterParameters = new ConverterParameters();
    CmdLineParser parser = new CmdLineParser(converterParameters);

    try {
      long startTime = System.currentTimeMillis();

      parser.parseArgument(args);
      String dateFormat = converterParameters.getDateFormat();
      if (StringUtils.isEmpty(converterParameters.getDateFormat())) {
        dateFormat = "yyyy.MM.dd HH:mm:ss.SSS";
      }

      outFileName = initOutFile(converterParameters);

      if (OutputFileType.TICKS.equals(converterParameters.getOutputFileType())) {

        try {
          writer = new FileWriter(outFileName, true);
          if (!converterParameters.getNoHeader()) {
            if (converterParameters.getUseMql()) {
              writer.write("<DATE>;<TIME>;<BID>;<ASK>;<LAST>;<VOLUME>\n"); //format
            } else {
              writer.write("symbol;time;price;volume;deal_id\n");
            }
          }

          processInputFile(new OrdersProcessorOnlyTicks(new TicksWriterActionListener(writer, dateFormat, converterParameters.getTimeFormat(), converterParameters.getUseMql(),
            converterParameters.getScale(), converterParameters.getStart(), converterParameters.getEnd())), converterParameters.getInputFile());
          writer.flush();
          writer.close();
        } catch(IOException e) {
          log.error("file write error", e);
        }

      } else if (OutputFileType.BOOKSTATE.equals(converterParameters.getOutputFileType())) {
        try {
          writer = new FileWriter(outFileName, true);
          if (!converterParameters.getNoHeader()) {
            if (converterParameters.getUseMql()) {
              writer.write("<DATE>;<TIME>;<BID>;<ASK>;<LAST>;<VOLUME>\n"); //format
            } else {
              writer.write("symbol;time;ask;bid;askvol;bidvol\n"); //format
            }
          }
          Integer timeQuant = converterParameters.getTimeQuant() != null ? converterParameters.getTimeQuant() : 0;
          processInputFile(new OrdersProcessorBookMap(new BookStateWriterActionListener(writer, dateFormat, converterParameters.getTimeFormat(), timeQuant, converterParameters.getUseMql(),
            converterParameters.getScale(), converterParameters.getStart(), converterParameters.getEnd())), converterParameters.getInputFile());
          writer.flush();
          writer.close();
        } catch(IOException e) {
          log.error("file write error", e);
        }

      } else if (OutputFileType.BARS.equals(converterParameters.getOutputFileType())) {

        if (converterParameters.getBarPeriod() == null) {
          throw new IllegalArgumentException("-period parameter required for -type=BARS");
        }

        TradePeriod period = TradePeriod.fromString(converterParameters.getBarPeriod().toString());

        if (StringUtils.isEmpty(converterParameters.getDateFormat())) {
          dateFormat = "yyyy.MM.dd HH:mm";
        }
        String timeFormat = converterParameters.getTimeFormat();

        try {
          writer = new FileWriter(outFileName, true);
          if (!converterParameters.getNoHeader()) {
            if (converterParameters.getUseMql()) {
              writer.write("<DATE>;<TIME>;<OPEN>;<HIGH>;<LOW>;<CLOSE>;<TICKVOL>;<VOL>;<SPREAD>\n"); //format
            } else {
              if (StringUtils.isEmpty(timeFormat)) {
                writer.write("<TICKER>;<DATE>;<OPEN>;<HIGH>;<LOW>;<CLOSE>;<VOL>\n"); //format
              } else {
                writer.write("<TICKER>;<DATE>;<TIME>;<OPEN>;<HIGH>;<LOW>;<CLOSE>;<VOL>\n"); //format
              }
            }
          }
          processInputFile(new OrdersProcessorBookMap(new BarsCollectorActionListener(
            writer,
            dateFormat,
            timeFormat,
            converterParameters.getUseMql(),
            period,
            converterParameters.getUseBookState(),
            converterParameters.getScale(),
            converterParameters.getStart(),
            converterParameters.getEnd(),
            converterParameters.getBarTime()
          )), converterParameters.getInputFile());
          writer.flush();
          writer.close();
        } catch(IOException e) {
          log.error("file write error", e);
        }

      }


      System.out.println("processingTime: " + (System.currentTimeMillis() - startTime));

    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
    }


  }

}
