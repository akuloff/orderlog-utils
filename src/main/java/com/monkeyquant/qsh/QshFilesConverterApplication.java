package com.monkeyquant.qsh;

import com.alex09x.qsh.reader.QshReaderFactory;
import com.alex09x.qsh.reader.type.OrdersLogRecord;
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
      parser.parseArgument(args);
      String dateFormat = converterParameters.getTimeFormat();
      outFileName = converterParameters.getOutputFile();

      if (converterParameters.getOutputFileType().equals(OutputFileType.TICKS)) {
        if (StringUtils.isEmpty(converterParameters.getTimeFormat())) {
          dateFormat = "yyyy.MM.dd HH:mm:ss.SSS";
        }

        if (StringUtils.isEmpty(converterParameters.getOutputFile())) {
          outFileName = converterParameters.getInputFile() + "_ticks.csv";
        }

        try {
          writer = new FileWriter(outFileName, true);
          writer.write("symbol;time;price;volume;deal_id\n");
          processInputFile(new OrdersProcessorOnlyTicks(new TicksWriterActionListener(writer, dateFormat)), converterParameters.getInputFile());
          writer.flush();
          writer.close();
        } catch(IOException e) {
          log.error("file write error", e);
        }

      }
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
    }
  }

}
