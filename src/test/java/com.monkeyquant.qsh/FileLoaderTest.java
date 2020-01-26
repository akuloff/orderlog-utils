package com.monkeyquant.qsh;

import com.alex09x.qsh.reader.QshReaderFactory;
import com.alex09x.qsh.reader.type.OrdersLogRecord;
import com.monkeyquant.qsh.listeners.StatsMarketActionListener;
import com.monkeyquant.qsh.model.IMarketActionListener;
import com.monkeyquant.qsh.model.IOrdersProcessor;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

//TODO fix tests

public class FileLoaderTest {

  private void processQshFile(String fpath) throws IOException {
    QshReaderFactory readerFactory = new QshReaderFactory();
    Iterator<OrdersLogRecord> reader = readerFactory.openPath(fpath);
    OrdersLogRecord rec;
    IMarketActionListener stats = new StatsMarketActionListener();
    IOrdersProcessor processor = new OrdersProcessorBookMap(stats);

    long trans = 0, totalRead = 0;
    while ( reader.hasNext() ) {
      rec = reader.next();
      totalRead ++;
      if (rec.isEndTransaction()) {
        trans ++;
      }
      processor.processOrderRecord(rec);
    }
    System.out.println("trans = " + trans);
    System.out.println("totalRead = " + totalRead);

    System.out.println("market stats: " + stats.toString());
//    System.out.println("processor map size: " + processor.getMapSize());
//    System.out.println("buy size: " + processor.getBookState().getBuySize() + " |sell size: " + processor.getBookState().getSellSize());
//    System.out.println("bstate counters, put: " + processor.getBookState().getPutCount() + " |set: " + processor.getBookState().getSetCount() + " |remove: " + processor.getBookState().getRemoveCount());

    //Utils.printBookState(processor.getBookState(), 2);

//    System.out.println("buy: ");
//    printTreeMap(processor.getBookState().getBuyPositions());
//
//    System.out.println("sell: ");
//    printTreeMap(processor.getBookState().getSellPositions());
  }

//  @Test
  public void testLoadQshOrdelog() throws Exception {
    long startTime = System.currentTimeMillis();

    File dir = new File("d:\\tester_data\\qsh_files\\tst");
    File[] directoryListing = dir.listFiles();
    if (directoryListing != null) {
      for (File child : directoryListing) {
        System.out.println(" --------------- " + " processing file: " + child.toString());
        processQshFile(child.toString());
      }
    }

    System.out.println("\n -------------- end all files --------------- ");
    long timeDiff = System.currentTimeMillis() - startTime;
    System.out.println("timeDiff = " + timeDiff);
  }

}
