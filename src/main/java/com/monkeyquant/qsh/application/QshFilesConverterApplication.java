package com.monkeyquant.qsh.application;

import com.alex09x.qsh.reader.QshReaderFactory;
import com.alex09x.qsh.reader.type.OrdersLogRecord;
import com.monkeyquant.qsh.listeners.BarsCollectorActionListener;
import com.monkeyquant.qsh.listeners.BookStateWriterActionListener;
import com.monkeyquant.qsh.listeners.TicksWriterActionListener;
import com.monkeyquant.qsh.model.IOrdersProcessor;
import com.monkeyquant.qsh.model.OutputFileType;
import com.monkeyquant.qsh.processor.OrdersProcessorBookMap;
import com.monkeyquant.qsh.processor.OrdersProcessorOnlyTicks;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

@Slf4j
public class QshFilesConverterApplication {

  private static void processInputFile(IOrdersProcessor ordersProcessor, String inputFileName) throws Exception {
    QshReaderFactory qshReaderFactory = new QshReaderFactory();
    Iterator<OrdersLogRecord> ordersLogRecordIterator = qshReaderFactory.openPath(inputFileName);
    ordersProcessor.init();
    while (ordersLogRecordIterator.hasNext()) {
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
          processInputFile(new OrdersProcessorOnlyTicks(new TicksWriterActionListener(writer, dateFormat, converterParameters)), converterParameters.getInputFile());
          writer.flush();
          writer.close();
        } catch(Exception e) {
          log.error("file write error", e);
        }

      } else if (OutputFileType.BOOKSTATE.equals(converterParameters.getOutputFileType())) {
        try {
          writer = new FileWriter(outFileName, true);
          processInputFile(new OrdersProcessorBookMap(new BookStateWriterActionListener(writer, dateFormat, converterParameters)), converterParameters.getInputFile());
          writer.flush();
          writer.close();
        } catch(Exception e) {
          log.error("file write error", e);
        }

      } else if (OutputFileType.BARS.equals(converterParameters.getOutputFileType())) {

        if (converterParameters.getBarPeriod() == null) {
          throw new IllegalArgumentException("-period parameter required for -type=BARS");
        }

        try {
          writer = new FileWriter(outFileName, true);
          processInputFile(new OrdersProcessorBookMap(new BarsCollectorActionListener(writer, converterParameters)), converterParameters.getInputFile());
          writer.flush();
          writer.close();
        } catch(Exception e) {
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
