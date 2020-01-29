package com.monkeyquant.qsh.application;

import com.alex09x.qsh.reader.QshReaderFactory;
import com.alex09x.qsh.reader.type.OrdersLogRecord;
import com.monkeyquant.qsh.model.IDataWriter;
import com.monkeyquant.qsh.model.IOrdersProcessor;
import com.monkeyquant.qsh.model.IOrdersProcessorFactory;
import com.monkeyquant.qsh.model.OutputFormatType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.CmdLineParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

@Component
@Slf4j
public class MainExportProcessComponent {

  @Autowired
  private ApplicationArguments args;

  @Autowired
  private IOrdersProcessorFactory ordersProcessorFactory;

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
    String outFileName, appender = "";

    String outfile = parameters.getOutputFile();
    if (!StringUtils.isEmpty(outfile)) {
      return outfile;
    }

    OutputFormatType fileType = parameters.getOutputFormatType();

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


  @PostConstruct
  private void startProcess() {
    FileWriter writer = null;
    String outFileName;
    ConverterParameters converterParameters = new ConverterParameters();
    CmdLineParser parser = new CmdLineParser(converterParameters);

    try {
      parser.parseArgument(args.getSourceArgs());

      if (OutputFormatType.BARS.equals(converterParameters.getOutputFormatType()) && converterParameters.getBarPeriod() == null) {
        throw new IllegalArgumentException("-period parameter required for -type=BARS");
      }

      outFileName = initOutFile(converterParameters);
      writer = new FileWriter(outFileName, true);
      IDataWriter dataWriter = new FileDataWriterImpl(writer);
      IOrdersProcessor ordersProcessor = ordersProcessorFactory.getOrdersProcessor(dataWriter, converterParameters);

      if (ordersProcessor != null) {
        try {
          long startTime = System.currentTimeMillis();
          writer = new FileWriter(outFileName, true);
          processInputFile(ordersProcessor, converterParameters.getInputFile());
          writer.flush();
          writer.close();
          log.info("processingTime: {}", (System.currentTimeMillis() - startTime));
        } catch (IOException e) {
          log.warn("file write error", e);
        }
      }

    } catch (Exception e) {
      log.warn(e.getMessage());
      System.out.println("\nusage params:\n");
      parser.printUsage(System.out);
    }
  }

}
