package com.monkeyquant.qsh.application;

import com.alex09x.qsh.reader.QshReaderFactory;
import com.alex09x.qsh.reader.type.OrdersLogRecord;
import com.monkeyquant.qsh.model.IOrdersProcessor;
import com.monkeyquant.qsh.model.IOrdersProcessorFactory;
import com.monkeyquant.qsh.model.OutputFormatType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.CmdLineParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

@Slf4j
@Component
public class MainExportCommandRunner implements CommandLineRunner {

  @Autowired
  private IOrdersProcessorFactory ordersProcessorFactory;

  private static void processInputFile(IOrdersProcessor ordersProcessor, String inputFileName, boolean init) throws Exception {
    QshReaderFactory qshReaderFactory = new QshReaderFactory();
    Iterator<OrdersLogRecord> ordersLogRecordIterator = qshReaderFactory.openPath(inputFileName);
    if (init) {
      ordersProcessor.init();
    }
    while (ordersLogRecordIterator.hasNext()) {
      OrdersLogRecord ordersLogRecord = ordersLogRecordIterator.next();
      ordersProcessor.processOrderRecord(ordersLogRecord);
    }
  }

  private static String initOutFile(ConverterParameters parameters, int counter){
    String outFileName;
    StringBuilder sb = new StringBuilder();

    String outfile = parameters.getOutputFile();
    if (!StringUtils.isEmpty(outfile)) {
      return outfile;
    }

    OutputFormatType fileType = parameters.getOutputFormatType();

    if (parameters.getTimeQuant() != null && parameters.getTimeQuant() > 0) {
      sb.append("_t").append(parameters.getTimeQuant().toString());
    }

    switch (fileType) {
      case TICKS:
        sb.append("_ticks.csv");
        break;
      case BARS:
        if (parameters.getBarPeriod() != null) {
          sb.append("_").append(parameters.getBarPeriod().toString());
        }
        sb.append("_bars.csv");
        break;
      case BOOKSTATE:
        sb.append("_book.csv");
        break;
      default:
        sb.append(".csv");
    }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmm");
    if (counter >= 0) {
      outFileName = String.format("out_%s_%s%s", sdf.format(new Date()), counter, sb.toString());
    } else {
      outFileName = String.format("out_%s%s", sdf.format(new Date()), sb.toString());
    }
    return outFileName;
  }


  private void sendFileToProcessor(IOrdersProcessor ordersProcessor, String inputFile, FileWriter writer, boolean init, boolean close) throws Exception {
    if (ordersProcessor != null) {
      try {
        long startTime = System.currentTimeMillis();
        processInputFile(ordersProcessor, inputFile, init);
        writer.flush();
        if (close) {
          writer.close();
        }
        log.info("file: {}, processing time: {}", inputFile, (System.currentTimeMillis() - startTime));
      } catch (IOException e) {
        log.warn("file write error", e);
      }
    }
  }

  @Override
  public void run(String... args) {
    FileWriter writer;
    String outFileName;
    ConverterParameters converterParameters = new ConverterParameters();
    CmdLineParser parser = new CmdLineParser(converterParameters);

    try {
      parser.parseArgument(args);

      if (OutputFormatType.BARS.equals(converterParameters.getOutputFormatType()) && converterParameters.getBarPeriod() == null) {
        throw new IllegalArgumentException("-period parameter required for -type=BARS");
      }

      outFileName = initOutFile(converterParameters, converterParameters.getBatchProcess() ? 0 : -1);
      writer = new FileWriter(outFileName, true);
      FileDataWriterImpl dataWriter = new FileDataWriterImpl(writer);
      IOrdersProcessor ordersProcessor = ordersProcessorFactory.getOrdersProcessor(dataWriter, converterParameters);

      if (converterParameters.getBatchProcess()) {
        String fileMask = converterParameters.getInputFile().replace("\\", "/");
        String dirName = ".";
        if (fileMask.contains("/")) {
          dirName = fileMask.substring(0, fileMask.lastIndexOf("/"));
          fileMask = fileMask.substring(fileMask.lastIndexOf("/") + 1);
        }
        log.info("batch processing, dir: {}, file mask: {}", dirName, fileMask);
        try (DirectoryStream<Path> dir = Files.newDirectoryStream(Paths.get(dirName), fileMask)) {
          int entryCounter = 0;
          for (Path entry : dir) {
            log.info("processing batch file: {}", entry);
            if (converterParameters.getOneFileProcess()) {
              sendFileToProcessor(ordersProcessor, entry.toString(), writer, entryCounter == 0, false);
            } else {
              if (entryCounter > 0) {
                writer = new FileWriter(initOutFile(converterParameters, entryCounter), true);
                dataWriter.setFileWriter(writer);
              }
              sendFileToProcessor(ordersProcessor, entry.toString(), writer, true, true);
            }
            entryCounter ++;
          }
        }
      } else {
        sendFileToProcessor(ordersProcessor, converterParameters.getInputFile(), writer, true, true);
      }

      ordersProcessor.end();

    } catch (Exception e) {
      log.warn(e.getMessage(), e);
      System.out.println("\nusage params:\n");
      parser.printUsage(System.out);
    }
  }

}
