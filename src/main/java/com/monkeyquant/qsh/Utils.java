package com.monkeyquant.qsh;

import com.alex09x.qsh.reader.type.DealType;
import com.alex09x.qsh.reader.type.OrdersLogRecord;
import com.monkeyquant.qsh.model.IBookState;
import com.monkeyquant.qsh.model.IOrdersProcessor;
import com.monkeyquant.qsh.model.MarketDealType;
import com.monkeyquant.qsh.model.PriceRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Utils {

  public static PriceRecord getBestForVolume(List<PriceRecord> priceRecordList, Integer needValue){
    Integer totalValue = 0;
    Double price = Double.NaN;
    if (needValue > 0 ) {
      for (PriceRecord priceRecord : priceRecordList) {
        if ( priceRecord.getValue() > 0 ) {
          totalValue += priceRecord.getValue();
          if (totalValue >= needValue) {
            price = priceRecord.getPrice();
            break;
          }
        }
      }
    }
    return PriceRecord.builder().price(price).value(totalValue).build();
  }

  private static String getLastTreeMapPositions(TreeMap<Double, Integer> map, int count){
    StringBuilder retString = new StringBuilder();
    double skey;
    int pval, l = 0;
    NavigableSet<Double> navigableSet = map.descendingKeySet();
    for (Iterator it = navigableSet.iterator(); it.hasNext();) {
      skey = (Double)it.next();
      pval = map.get(skey);
      if ( pval > 0 ) {
        retString.append("        ").append(skey).append(" ").append(pval).append("\n");
        l ++;
        if (l >= count) break;
      }
    }
    return retString.toString();
  }

  private static String getFirstTreeMapPositions(TreeMap<Double, Integer> map, int count, boolean stringsInReverse){
    StringBuilder retString = new StringBuilder();
    double skey;
    int pval, l = 0;
    for(Map.Entry<Double, Integer> e: map.entrySet()){
      skey = e.getKey();
      pval = e.getValue();
      if ( pval > 0){
        if (stringsInReverse) {
          retString.insert(0, "        " + skey + " " + pval + "\n");
        } else {
          retString.append("        ").append(skey).append(" ").append(pval).append("\n");
        }
        l ++;
        if (l >= count) break;
      }
    }
    return retString.toString();
  }

  public static void printTreeMap(TreeMap<Double, Integer> map){
    double skey;
    int pval, l = 0;
    for(Map.Entry<Double, Integer> e: map.entrySet()){
      skey = e.getKey();
      pval = e.getValue();
      if ( pval > 0){
        System.out.println(l + " " + skey + " " + pval);
        l ++;
      }
    }
  }


  public static String getStringBookState2(IBookState bstate, int max_len){
    String retString = "";
    retString = retString + "sell:\n";
    //retString = retString + getFirstTreeMapPositions(bstate.getAskPositions(max_len), max_len, true);
    retString = retString + "buy:" + "\n";
    //retString = retString + getLastTreeMapPositions(bstate.getBidPositions(max_len), max_len);
    return retString;
  }

  public static void printBookState(IBookState bstate, int max_len){
    System.out.println(getStringBookState2(bstate, max_len));
  }

  private static void printPriceList(List<PriceRecord> priceRecordList){
    int i = 0;
    for (PriceRecord record : priceRecordList){
      System.out.println("i: " + i + " |price: " + record.getPrice() + " |value: " + record.getValue());
      i ++;
    }
  }

  //стакан на определенное время
  public static void printBookOnTime(IOrdersProcessor ordersProcessor, Iterator<OrdersLogRecord>  qreader, int book_size, Calendar cal, int hour, int minute, int sec){
    while ( qreader.hasNext()  ) {
      OrdersLogRecord rec = qreader.next();
      ordersProcessor.processOrderRecord(rec);
      cal.setTimeInMillis(rec.getTime().getTime());
      if ( rec.isEndTransaction() && cal.get(Calendar.HOUR_OF_DAY) >= hour && cal.get(Calendar.MINUTE) >= minute && cal.get(Calendar.SECOND) >= sec  ) {
        //printBookState(ordersProcessor, book_size);
        break;
      }
    }
  }


  public static void bookImportTest(String fileName, char delim, String symb_code){
    CSVRecord record;
    String symbol, askp, bidp;

    try {
      Iterator<CSVRecord> records = CSVFormat.RFC4180.withHeader().withDelimiter(delim).parse(new BufferedReader(new FileReader(fileName))).iterator();
      while( records.hasNext() ) {
        record = records.next();
        symbol = record.get("symbol");

        if (symbol.equals(symb_code)){
          askp = record.get("ask");
          bidp = record.get("bid");
          System.out.println(askp + "\t" + bidp);
        }

      }
    } catch (IOException ex) {
      System.out.println(ex.getMessage());
    }
  }

  public static void printOrderRecord(OrdersLogRecord rec){
    System.out.println("ord: " + rec.getOrderId() +  " |price: " + rec.getOrderPrice() +
      " |vol: " + rec.getVolume() + " |rest: " + rec.getRestVolume() + " |d.id: " + rec.getDealId() + " |d.price: " + rec.getDealPrice() + " |OI: " + rec.getOpenInterest() +
      " |quote: " + rec.isQuote() + " |add: " + rec.isAdd() + " |move: " + rec.isMoved() +  " |fill: " + rec.isFill() +
      " |is_buy: " + (rec.getType() == DealType.BUY) +  " |quote: " + rec.isQuote() + " |end: " + rec.isEndTransaction() + " |cancel: " + rec.isCanceled() + " |cross: " + rec.isCrossTrade() +  " |nonsys: " + rec.isNonSystem() +  " |counter: " + rec.isCounter() + " |h.rest: " + ((rec.getHeaderFlags() & 16) > 0));
  }


  public static MarketDealType fromQshType(DealType dealType){
    switch (dealType) {
      case BUY:
        return MarketDealType.BUY;
      case SELL:
        return MarketDealType.SELL;
      default:
        return MarketDealType.UNKNOWN;
    }
  }

  public static MarketDealType fromQshTypeReverse(DealType dealType){
    switch (dealType) {
      case BUY:
        return MarketDealType.SELL;
      case SELL:
        return MarketDealType.BUY;
      default:
        return MarketDealType.UNKNOWN;
    }
  }


}
