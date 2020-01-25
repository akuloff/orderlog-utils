package com.monkeyquant.qsh;

import com.alex09x.qsh.reader.QshReaderFactory;
import com.alex09x.qsh.reader.type.OrdersLogRecord;
import com.monkeyquant.jte.primitives.model.PriceRecord;
import com.monkeyquant.qsh.model.IOrdersProcessor;
import com.monkeyquant.qsh.model.MapBookState;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.TimeZone;

@Log4j
public class QshFileProcessor {
    private IOrdersProcessor ordersProcessor;
    private QshReaderFactory rfactory1 = new QshReaderFactory();
    private SimpleDateFormat formatter = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS");

    @Getter
    @Setter
    private String renameInstrumentTo = "";

    @Getter
    @Setter
    private Integer needVolume = 1;

    @Getter
    @Setter
    private int decimalScale = 8;

    public QshFileProcessor(IOrdersProcessor ordersProcessor) {
        this.ordersProcessor = ordersProcessor;
    }

    private boolean checkTime(Calendar cal, int start_time, int end_time){
        boolean rval = false;
        int cur_time;
        cur_time = cal.get(Calendar.HOUR_OF_DAY)*60 + cal.get(Calendar.MINUTE);
        if ( cur_time >= start_time && (cur_time < end_time || end_time == 0) ) {
            rval = true;
        }
        return rval;
    }

    public void testProcessFile(String fpath, int msec_quant, int start_time, int end_time) throws IOException{
        Iterator<OrdersLogRecord> reader1;
        Calendar cal = new GregorianCalendar();
        long cur_msec = 0;
        OrdersLogRecord rec1 = null;
        long time1 = 0;
        boolean is_read = false;
        int r_cnt = 0; //прочитано записей в одном кванте времени
        long totalRead = 0;
        double last_ask = 0, last_bid = 0;
        long bufSize = 1000000;

        TimeZone tz = TimeZone.getTimeZone("GMT+3");
        cal.setTimeZone(tz);

        formatter.setCalendar(cal);
        formatter.setTimeZone(cal.getTimeZone());

        MapBookState bstate = null;

        reader1 = rfactory1.openPath(fpath);
        if ( reader1.hasNext() ) {
            rec1 = reader1.next();
            totalRead ++;
            time1 = rec1.getTime().getTime();
        }

        if (msec_quant > 0) {
            cur_msec = (time1/msec_quant)*msec_quant;
        } else {
            cur_msec = time1;
        }

        int period;
        period = msec_quant/60000;

        //for (i=0; i<max_i; i++) {

        boolean has_next = true;
        int total_out = 0;

        while( has_next ) {
            is_read = false;
            if ( rec1.getTime().getTime() < cur_msec || msec_quant == 0) {
                //обрабатываем последнюю запись - она не была обработана, только прочитано время, и оно превышало отметку шага
                ordersProcessor.processOrderRecord(rec1);

                has_next = false;
                while ( reader1.hasNext() ) {
                    has_next = true;
                    rec1 = reader1.next();
                    totalRead ++;
                    if ( rec1.getTime().getTime() > cur_msec || msec_quant == 0 ) { //время превышает отметку шага
                        is_read = true;
                        break;
                    } else {
                        ordersProcessor.processOrderRecord(rec1);
                        time1 = rec1.getTime().getTime();
                    }
                    r_cnt ++;
                }
            }

            if ( is_read ) {
                if (msec_quant > 0) {
                    cal.setTimeInMillis(cur_msec);
                } else {
                    cal.setTimeInMillis(rec1.getTime().getTime());
                }

                double b_ask[], b_bid[];

                if ( checkTime(cal, start_time, end_time) ) {
                    String bookString;
                    //bookString = ord_proc1.getStringBookState(20);

                    //bstate = ordersProcessor.getBookState();


                    //b_ask = ord_proc1.getBestAskForVolume(needVolume);
                    //b_bid = ord_proc1.getBestBidForVolume(needVolume);

                    /*
                    BigDecimal bd1, bd2;
                    bd1 = new BigDecimal(b_ask[0]).setScale(2, RoundingMode.CEILING);
                    bd2 = new BigDecimal(b_bid[0]).setScale(2, RoundingMode.CEILING);

                    boolean do_write = true;
                    if ( msec_quant == 0) {
                        if (last_ask != b_ask[0] || last_bid != b_bid[0]) {
                            last_ask = b_ask[0];
                            last_bid = b_bid[0];
                        } else {
                            do_write = false;
                        }
                    }
                    */
                }

                r_cnt = 0;
            }
            cur_msec += msec_quant;

            if(totalRead % bufSize == 0 && bstate != null) {
//                System.out.println("  readed ... " + totalRead + " |sells: " + bstate.getSellPositions().size() + " |buys: " + bstate.getBuyPositions().size() + " |time: " + dateFormat.format(cal.getTime()));
//                testPrintBook(bstate, 10);

                PriceRecord ask, bid;
//                ask = ordersProcessor.getBestAskForVolume(20);
//                bid = ordersProcessor.getBestBidForVolume(20);
                //System.out.println("        >>>    ask: " + ask.getPrice() + " | " + ask.getValue() + " |bid: " + bid.getPrice() + " | " + bid.getValue());

            }
        }

        System.out.println("total readed ... " + totalRead);

    }

    /**
     *
     * @param fpath
     * @param outfile
     * @param msec_quant
     * @param start_time
     * @param end_time
     */
    public void processFile(String fpath, String outfile, int msec_quant, int start_time, int end_time) throws IOException{
        Iterator<OrdersLogRecord> reader1;
        Calendar cal = new GregorianCalendar();
        long cur_msec = 0;
        OrdersLogRecord rec1 = null;
        long time1 = 0;
        boolean is_read = false;
        int r_cnt = 0; //прочитано записей в одном кванте времени
        long totalRead = 0;
        double last_ask = 0, last_bid = 0;
        long bufSize = 1000000;

        FileWriter writer;

        TimeZone tz = TimeZone.getTimeZone("GMT+3");
        cal.setTimeZone(tz);

        formatter.setCalendar(cal);
        formatter.setTimeZone(cal.getTimeZone());

            writer = new FileWriter(outfile, true)  ;
            writer.write("symbol;period;time;ask;bid;askvol;bidvol\n"); //format

            reader1 = rfactory1.openPath(fpath);
            if ( reader1.hasNext() ) {
                rec1 = reader1.next();
                totalRead ++;
                time1 = rec1.getTime().getTime();
            }

            if (msec_quant > 0) {
                cur_msec = (time1/msec_quant)*msec_quant;
            } else {
                cur_msec = time1;
            }

            int period;
            period = msec_quant/60000;

            //for (i=0; i<max_i; i++) {

            boolean has_next = true;
            boolean transactionPassed = false;
            int total_out = 0;

            while( has_next ) {
                is_read = false;
                if ( rec1.getTime().getTime() < cur_msec || msec_quant == 0) {

                    //обрабатываем последнюю запись - она не была обработана, только прочитано время, и оно превышало отметку шага
                    ordersProcessor.processOrderRecord(rec1);

                    has_next = false;
                    transactionPassed = false;
                    while ( reader1.hasNext() ) {
                        has_next = true;
                        rec1 = reader1.next();
                        totalRead ++;
                        if ( (rec1.getTime().getTime() > cur_msec || msec_quant == 0) && transactionPassed) { //время превышает отметку шага
                            is_read = true;
                            break;
                        } else {
                            ordersProcessor.processOrderRecord(rec1);
                            time1 = rec1.getTime().getTime();
                            transactionPassed = rec1.isEndTransaction();
                        }
                        r_cnt ++;
                    }
                }

                if ( is_read ) {
                    //String cal_time = "";
                    if (msec_quant > 0) {
                        cal.setTimeInMillis(cur_msec);
                    } else {
                        cal.setTimeInMillis(rec1.getTime().getTime());
                    }
                    //cal_time = cal_time + cal.get(Calendar.HOUR_OF_DAY) + ":" + cal.get(Calendar.MINUTE);
                    //System.out.println("i: " + i +  "\t" + rec1.getSymbol() + "\t\t" + time1 + "\t" + "r_cnt: " + r_cnt + "\t\t" + cur_msec + "\t" +  dateFormat.format(cal.getTime()) + "\t" + "\t" + ord_proc1.getBestAsk()[0] + "\t" + ord_proc1.getBestBid()[0] + "\t" + cal_time);

                    PriceRecord b_ask, b_bid;

                    if ( checkTime(cal, start_time, end_time) ) {

//                        b_ask = ordersProcessor.getBestAskForVolume(needVolume);
//                        b_bid = ordersProcessor.getBestBidForVolume(needVolume);

                        BigDecimal bd1, bd2;

//                        bd1 = new BigDecimal(b_ask.getPrice()).setScale(decimalScale, RoundingMode.HALF_DOWN);
//                        bd2 = new BigDecimal(b_bid.getPrice()).setScale(decimalScale, RoundingMode.HALF_DOWN);

                        boolean do_write = true;
                        if ( msec_quant == 0) {
//                            if (last_ask != b_ask.getPrice() || last_bid != b_bid.getPrice()) {
//                                last_ask = b_ask.getPrice();
//                                last_bid = b_bid.getPrice();
//                            } else {
//                                do_write = false;
//                            }
                        }

                        if (do_write) {
                            String outs = "";
                            if (!"".equals(this.renameInstrumentTo)) {
                                outs = this.renameInstrumentTo;
                            } else {
                                outs = rec1.getSymbol();
                            }
                            //outs = outs + ";" + period + ";" + dateFormat.format(cal.getTime()) + ";" + bd1 + ";" + bd2 + ";" + b_ask.getValue() + ";" + b_bid.getValue();
                            //System.out.println(outs);
                            writer.write(outs + "\n");
                            total_out ++;
                        }
                    }

                    r_cnt = 0;
                }

                cur_msec += msec_quant;

                if(totalRead % bufSize == 0) {
                    System.out.println("total readed ... " + totalRead + " ,do flush");
                    writer.flush();
                }
            }

            writer.flush();
            writer.close();

            System.out.println("write completed, total lines: " + total_out + " |totalRead: " + totalRead);
    }

    public void processFile(String fpath, String outfile, int msec_quant) throws IOException{
        processFile(fpath, outfile, msec_quant, 0, 0);
    }

    private static String getRecordString(OrdersLogRecord rec){
        StringBuilder sb = new StringBuilder();
        sb.append("order: ").append(rec.getOrderId()).append(" |deal: ").append(rec.getDealId()).append(" |o.price: ").append(rec.getOrderPrice()).append(" |d.price: ").append(rec.getDealPrice());
        String flagsString = Integer.toString(rec.getOrderFlags(), 2);
        sb.append(" |volume: ").append(rec.getVolume()).append(" |flags: ");
        for (int i=flagsString.length(); i<16; i++) sb.append('0');
        sb.append(flagsString);
        return sb.toString();
    }

    public void processFileTicks(String fpath, String outfile, int start_time, int end_time) throws IOException{
        Iterator<OrdersLogRecord> reader1;
        Calendar cal = new GregorianCalendar();
        OrdersLogRecord rec1 = null;
        long totalRead = 0;
        long bufSize = 1000000;
        FileWriter writer;

        cal.setTimeZone(TimeZone.getTimeZone("GMT+3"));
        formatter.setCalendar(cal);
        formatter.setTimeZone(cal.getTimeZone());

        writer = new FileWriter(outfile, true);
        writer.write("symbol;time;price;volume;deal_id\n");

        reader1 = rfactory1.openPath(fpath);
        int total_out = 0;

        long lastDealId = 0;

        while ( reader1.hasNext() ) {
            rec1 = reader1.next();
            totalRead ++;

            boolean do_write = false;

            if(rec1.isFill()
                    && !rec1.isCanceled() && !rec1.isCrossTrade() && !rec1.isMoved() && !rec1.isNonSystem() && rec1.getDealId() > 0 && !rec1.isCounter()
            ){
                if (rec1.getDealId() != lastDealId) {
                    lastDealId = rec1.getDealId();

                    cal.setTimeInMillis(rec1.getTime().getTime());
                    do_write = true;
                }
            }

            if (do_write && !checkTime(cal, start_time, end_time)) {
                do_write = false;
            }

            if (do_write) {
                String outs = "";
                if (!"".equals(this.renameInstrumentTo)) {
                    outs = this.renameInstrumentTo;
                } else {
                    outs = rec1.getSymbol();
                }
                outs = outs + ";" + formatter.format(cal.getTime()) + ";" + rec1.getDealPrice() + ";" + rec1.getVolume() + ";" + rec1.getDealId();
                writer.write(outs + "\n");
                total_out ++;
            }

            if(totalRead % bufSize == 0) {
                System.out.println("total readed ... " + totalRead + " ,do flush");
                writer.flush();
            }
        }

        writer.flush();
        writer.close();

        System.out.println("write completed, total lines: " + total_out + " |totalRead: " + totalRead);
    }

}
