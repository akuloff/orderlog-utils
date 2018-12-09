package com.monkeyquant.qsh;

import com.alex09x.qsh.reader.QshReaderFactory;
import com.alex09x.qsh.reader.type.OrdersLogRecord;
import com.monkeyquant.jte.primitives.model.TradePeriod;
import com.monkeyquant.qsh.model.IOrdersProcessor;
import com.monkeyquant.qsh.model.OutputFileType;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.FileWriter;
import java.io.IOException;
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

  public static void main(String[] args) {
    FileWriter writer;
    String outFileName;

    ConverterParameters converterParameters = new ConverterParameters();
    CmdLineParser parser = new CmdLineParser(converterParameters);

    try {
      long startTime = System.currentTimeMillis();

      parser.parseArgument(args);
      String dateFormat = converterParameters.getTimeFormat();
      if (StringUtils.isEmpty(converterParameters.getTimeFormat())) {
        dateFormat = "yyyy.MM.dd HH:mm:ss.SSS";
      }
      outFileName = converterParameters.getOutputFile();

      if (OutputFileType.TICKS.equals(converterParameters.getOutputFileType())) {

        if (StringUtils.isEmpty(converterParameters.getOutputFile())) {
          outFileName = converterParameters.getInputFile() + "_ticks.csv";
        }

        try {
          writer = new FileWriter(outFileName, true);
          if (converterParameters.getUseMql()) {
            writer.write("<DATE>;<TIME>;<BID>;<ASK>;<LAST>;<VOLUME>\n"); //format
          } else {
            writer.write("symbol;time;price;volume;deal_id\n");
          }

          processInputFile(new OrdersProcessorOnlyTicks(new TicksWriterActionListener(writer, dateFormat, converterParameters.getUseMql())), converterParameters.getInputFile());
          writer.flush();
          writer.close();
        } catch(IOException e) {
          log.error("file write error", e);
        }

      } else if (OutputFileType.BOOKSTATE.equals(converterParameters.getOutputFileType())) {
        if (StringUtils.isEmpty(converterParameters.getOutputFile())) {
          outFileName = converterParameters.getInputFile() + "_book.csv";
        }
        try {
          writer = new FileWriter(outFileName, true);
          if (converterParameters.getUseMql()) {
            writer.write("<DATE>;<TIME>;<BID>;<ASK>;<LAST>;<VOLUME>\n"); //format
          } else {
            writer.write("symbol;time;ask;bid;askvol;bidvol\n"); //format
          }
          Integer timeQuant = converterParameters.getTimeQuant() != null ? converterParameters.getTimeQuant() : 0;
          processInputFile(new OrdersProcessorBookMap(new BookStateWriterActionListener(writer, dateFormat, timeQuant, converterParameters.getUseMql())), converterParameters.getInputFile());
          writer.flush();
          writer.close();
        } catch(IOException e) {
          log.error("file write error", e);
        }

      } else if (OutputFileType.BARS.equals(converterParameters.getOutputFileType())) {

        if (StringUtils.isEmpty(converterParameters.getOutputFile())) {
          outFileName = converterParameters.getInputFile() + "_bars.csv";
        }

        TradePeriod period = TradePeriod.fromString(converterParameters.getBarPeriod().toString());

        try {
          writer = new FileWriter(outFileName, true);
          if (converterParameters.getUseMql()) {
            writer.write("<DATE>;<TIME>;<OPEN>;<HIGH>;<LOW>;<CLOSE>;<TICKVOL>;<VOL>;<SPREAD>\n"); //format
          } else {
            writer.write("<TICKER>;<DATE>;<OPEN>;<HIGH>;<LOW>;<CLOSE>;<VOL>\n"); //format
          }
          processInputFile(new OrdersProcessorBookMap(new BarsCollectorActionListener(writer, dateFormat, converterParameters.getUseMql(), period)), converterParameters.getInputFile());
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
