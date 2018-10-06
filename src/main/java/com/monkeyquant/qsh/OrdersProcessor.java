package com.monkeyquant.qsh;

import com.alex09x.qsh.reader.type.DealType;
import com.alex09x.qsh.reader.type.OrdersLogRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class OrdersProcessor {
    private BookState bstate = new BookState();
    private HashMap<Long, OrdersLogRecord> orders_map = new HashMap<>();

    public static void print_rec(OrdersLogRecord rec){
        System.out.println("ord: " + rec.getOrderId() +  " |price: " + rec.getOrderPrice() +
                " |vol: " + rec.getVolume() + " |rest: " + rec.getRestVolume() + " |d.id: " + rec.getDealId() + " |d.price: " + rec.getDealPrice() + " |OI: " + rec.getOpenInterest() +
                " |quote: " + rec.isQuote() + " |add: " + rec.isAdd() + " |move: " + rec.isMoved() +  " |fill: " + rec.isFill() +
                " |is_buy: " + (rec.getType() == DealType.BUY) +  " |quote: " + rec.isQuote() + " |end: " + rec.isEndTransaction() + " |cancel: " + rec.isCanceled() + " |cross: " + rec.isCrossTrade() +  " |nonsys: " + rec.isNonSystem() +  " |counter: " + rec.isCounter() + " |h.rest: " + ((rec.getHeaderFlags() & 16) > 0));
    }

    public double[] getBestForVolume(TreeMap<Double, Integer> tmap, double needValue){
        double[] rval = new double[2];
        double totalValue = 0;
        if (needValue > 0 ) {
            for (Map.Entry<Double, Integer> e : tmap.entrySet()) {
                if ( e.getValue() > 0 ) {
                    totalValue += e.getValue();
                    if (totalValue >= needValue) {
                        rval[0] = e.getKey();
                        rval[1] = totalValue;
                        break;
                    }
                }
            }
        }
        return rval;
    }

    /**
     * получает лучший ask для нужного объема
     * @param needValue
     * @return
     */
    public double[] getBestAskForVolume(double needValue){
        return getBestForVolume(bstate.getSellPositions(), needValue);
    }

    /**
     * получает лучший текущий ask (без учета объема)
     * @return
     */
    public double[] getBestAsk(){
        return getBestAskForVolume(1);
    }

    /**
     * получает лучший bid для нужного объема
     * @param needValue
     * @return
     */
    public double[] getBestBidForVolume(double needValue){
        return getBestForVolume(bstate.getBuyPositions(), needValue);
    }


    /**
     * получает лучший текущий bid (без учета объема)
     * @return
     */
    public double[] getBestBid(){
        return getBestBidForVolume(1);
    }


    public void printBookState(int max_len){
        System.out.println(getStringBookState(max_len));
    }

    public String getStringBookState(int max_len){
        int l = 0;
        NavigableSet buyset;
        double skey;
        int pval;
        double sprices[];
        int spos[];
        String retString = "";

        sprices = new double[max_len];
        spos = new int[max_len];

        l = 0;
        retString = retString + "sell:\n";
        for(Map.Entry<Double, Integer> e: bstate.getSellPositions().entrySet()){
            skey = e.getKey();
            pval = e.getValue();
            if ( pval > 0){
                l ++;
                sprices[max_len - l] = skey;
                spos[max_len - l] = pval;
                if (l >= max_len) break;
            }
        }

        for(int i=0; i<max_len; i++) retString = retString + "        " + sprices[i] + " " + spos[i] + "\n";

        l = 0;
        retString = retString + "buy:" + "\n";
        buyset = bstate.getBuyPositions().descendingKeySet();
        for (Iterator it = buyset.iterator(); it.hasNext();) {
            skey = (Double)it.next();
            pval = bstate.getBuyPositions().get(skey);
            if ( pval > 0 ) {
                retString = retString + "        " + skey + " " + pval + "\n";
                l ++;
                if (l >= max_len) break;
            }
        }

        return retString;
    }

    public BookState getBstate() {
        return bstate;
    }

    public void setBstate(BookState bstate) {
        this.bstate = bstate;
    }

    //сбор статистики для отладки
    int total_recs =0, unk_count = 0;
    int add_cnt = 0, fill_cnt = 0, cancel_cnt = 0, quote_cnt = 0, move_cnt = 0, nonsys_cnt = 0, buy_cnt = 0, sell_cnt = 0;
    int total_fill = 0, total_end_fill = 0, fill_no_order = 0, fill_novolume = 0, fill_notfound = 0, fill_notmatch = 0;
    int noprice_cnt = 0, cross_cnt = 0, fill_noprice = 0, cancel_notfound = 0, only_move_cnt = 0, cross_notfound = 0;
    int move_add = 0, move_fill = 0, move_notfound = 0, move_price = 0, move_volume = 0, move_type = 0;
    int newmove_price = 0, newmove_value = 0, move_remove = 0, nodeal_price = 0, move_exists = 0, end_trans = 0;
    int counter_cnt = 0, counter_only = 0, counter_end = 0;

    //вывод статистики (для отладки)
    public void printStats(){
        System.out.println("total_recs = " + total_recs + " |unk_count: " + unk_count + " |add_cnt: " + add_cnt + " |fill_cnt: " + fill_cnt + " |cancel_cnt: " + cancel_cnt + " |quote_cnt: " + quote_cnt + " |move_cnt: " + move_cnt + " |nonsys_cnt: " + nonsys_cnt);
        System.out.println("buy_cnt: " + buy_cnt + " |sell_cnt: " + sell_cnt + " |buy+sell: " + (buy_cnt + sell_cnt));
        System.out.println("total_fill: " + total_fill +  " |total_end_fill: " + total_end_fill);
        System.out.println("noprice_cnt: " + noprice_cnt + " |fill_no_order: " + fill_no_order + " |fill_novolume: " + fill_novolume + " |fill_notfound: " + fill_notfound + " |fill_notmatch: " + fill_notmatch);
        System.out.println("cross_cnt: " + cross_cnt + " |fill_noprice: " + fill_noprice + " |cancel_notfound: " + cancel_notfound + " |cross_notfound: " + cross_notfound);
        System.out.println("only_move_cnt: " + only_move_cnt + " |move_add: " + move_add + " |move_fill: " + move_fill);
        System.out.println("move_notfound: " + move_notfound + " |move_price: " + move_price + " |move_volume: " + move_volume + " |move_type: " + move_type + " |move_exists: " + move_exists);
        System.out.println("newmove_price: " + newmove_price + " |newmove_value: " + newmove_value + " |move_remove: " + move_remove + " |nodeal_price: " + nodeal_price + " |end_trans: " + end_trans);
        System.out.println("counter_cnt: " + counter_cnt + " |counter_only: " + counter_only + " |counter_end: " + counter_end);
    }

    //со сбором статистики (для отладки)
    public boolean processOrderRecordStats(OrdersLogRecord rec, java.sql.Timestamp beg_time, java.sql.Timestamp end_time){
        OrdersLogRecord oldrec;
        boolean rval = false;

        if ( (beg_time == null || rec.getTime().after(beg_time)) && (end_time == null || rec.getTime().before(end_time)) ) {
            total_recs ++;
            rval = true;

            if ( rec.isNewSession() ) {
                orders_map.clear();
                bstate.clearAll();
            }

            long ord_id = rec.getOrderId();

            if ( ord_id > 0 ) {

                if ( !rec.isCanceled() ) {

                    if ( rec.isMoved() ) {
                        move_cnt ++;

                        if ( orders_map.containsKey(ord_id) ){ //заяка уже была добавлена
                            oldrec = orders_map.get(ord_id);

                            if (rec.getRestVolume() == 0){ //объема в заявке не осталось (значит пеернос заявки - то же что и удаление и постановка новой)
                                orders_map.remove(ord_id);
                                move_remove ++;
                                bstate.addForDealType(ord_id, oldrec.getType(), oldrec.getOrderPrice(), oldrec.getRestVolume()*(-1));
                            } else {
                                bstate.addForDealType(ord_id, oldrec.getType(), oldrec.getOrderPrice(), (oldrec.getRestVolume() - rec.getRestVolume())*(-1));
                            }

                            move_exists ++;
                        }
                    }

                    if ( rec.isAdd() && !rec.isNonSystem()){ //добавление новой заявки
                        if ( !orders_map.containsKey(ord_id) ){
                            orders_map.put(ord_id, rec);
                            bstate.addForDealType(ord_id, rec.getType(), rec.getOrderPrice(), rec.getRestVolume());
                        }
                    }else if ( rec.isFill() ){ //сделка по заявке
                        total_fill ++ ;
                        if (rec.getRestVolume() == 0) total_end_fill ++;

                        if ( orders_map.containsKey(ord_id) ) {
                            oldrec = orders_map.get(ord_id);
                            if ( rec.getRestVolume() > oldrec.getVolume() ) {
                                System.out.println("!rest is bigger than initial volume: " + ord_id);
                            }

                            int new_vol = 0;
                            int oldrest = 0;

                            oldrest = oldrec.getRestVolume();
                            new_vol = oldrest - rec.getVolume();

                            if ( new_vol > 0 ) {
                                oldrec.setRestVolume(new_vol);
                                orders_map.put(ord_id, oldrec);
                            } else { //ордер полностью исполнился
                                orders_map.remove(ord_id);
                            }

                            if (new_vol != rec.getRestVolume()){
                                //System.out.println("fill, new vol and rest not match, ord: " + ord_id + " |new_vol: " + new_vol + " |new.rest: " + rec.getRestVolume() + " |rec_vol: " + rec.getVolume() + " |old.rest: " + oldrest);
                                fill_notmatch ++;
                            }

                            bstate.addForDealType(ord_id, oldrec.getType(), oldrec.getOrderPrice(), rec.getVolume()*(-1));

                        } else {
                            fill_notfound ++;
                        }

                    }

                } else if ( rec.isCanceled() ){ //отмена (удаление) заявки
                    if ( orders_map.containsKey(ord_id) ){
                        oldrec = orders_map.get(ord_id);
                        bstate.addForDealType(ord_id, oldrec.getType(), oldrec.getOrderPrice(), oldrec.getRestVolume()*(-1));
                        orders_map.remove(ord_id);
                    } else {
                        //System.out.println("!!! can not find order for remove: " + ord_id);
                        cancel_notfound ++ ;
                    }

                    cancel_cnt ++;
                }

                if ( rec.isCrossTrade() ) {
                    cross_cnt ++;
                    //System.out.println("cross, ord: " + ord_id + " |is_add: " + rec.isAdd() + " |is_cancel: " + rec.isCanceled() + " |is_fill: " + rec.isFill());
                    if ( !orders_map.containsKey(ord_id) ) {
                        cross_notfound ++;
                    } else {
                        oldrec = orders_map.get(ord_id);
                        bstate.addForDealType(ord_id, oldrec.getType(), oldrec.getOrderPrice(), oldrec.getRestVolume()*(-1));
                    }

                    orders_map.remove(ord_id);
                }


                if ( rec.isCounter() ){
                    counter_cnt ++;
                    if (!rec.isAdd() && !rec.isFill() && !rec.isQuote() && !rec.isMoved() && !rec.isCrossTrade() && !rec.isCanceled()) {
                        counter_only ++;

                        if ( rec.isEndTransaction() ) {
                            counter_end ++;
                            if ( orders_map.containsKey(ord_id) ) { //ликвидация сделки
                                oldrec = orders_map.get(ord_id);
                                bstate.addForDealType(ord_id, oldrec.getType(), oldrec.getOrderPrice(), (oldrec.getRestVolume() - rec.getRestVolume())*(-1));
                            }
                        }
                    }
                }

            }

            if ( rec.isFill() && ((rec.getHeaderFlags() & 4) == 0)) {
                noprice_cnt ++;
            }

            if ( rec.isAdd() ) add_cnt ++;
            if ( rec.isFill() ) fill_cnt ++;
            if ( rec.isQuote() ) quote_cnt ++;

            if (rec.isNonSystem()) nonsys_cnt ++;

            if (rec.getType() == DealType.BUY) {
                buy_cnt ++;
            } else if (rec.getType() == DealType.SELL){
                sell_cnt ++;
            } else if ( rec.getType() == DealType.UNKNOWN ) {
                unk_count ++;
            }

            if (rec.getDealPrice() == 0) nodeal_price ++;

            if (rec.isEndTransaction()) end_trans ++;

        }

        return rval;
    }

    public void processOrderRecord(OrdersLogRecord rec){
        OrdersLogRecord oldrec;

        //новая сессия, очистка массивов
        if ( rec.isNewSession() ) {
            orders_map.clear();
            bstate.clearAll();
        }

        long ord_id = rec.getOrderId();

        if ( ord_id > 0 ) {
            if ( !rec.isCanceled() ) {
                if ( rec.isMoved() ) {
                    if ( orders_map.containsKey(ord_id) ){ //заяка уже была добавлена
                        oldrec = orders_map.get(ord_id);
                        if (rec.getRestVolume() == 0){ //объема в заявке не осталось (значит пеернос заявки - то же что и удаление и постановка новой)
                            orders_map.remove(ord_id);
                            bstate.addForDealType(ord_id, oldrec.getType(), oldrec.getOrderPrice(), oldrec.getRestVolume()*(-1));
                        } else {
                            bstate.addForDealType(ord_id, oldrec.getType(), oldrec.getOrderPrice(), (oldrec.getRestVolume() - rec.getRestVolume())*(-1));
                        }
                    }
                }

                if ( rec.isAdd() && !rec.isNonSystem()){ //добавление новой заявки
                    if ( !orders_map.containsKey(ord_id) ){
                        orders_map.put(ord_id, rec);
                        bstate.addForDealType(ord_id, rec.getType(), rec.getOrderPrice(), rec.getRestVolume());
                    }
                }else if ( rec.isFill() ){ //сделка по заявке

                    if ( orders_map.containsKey(ord_id) ) {
                        oldrec = orders_map.get(ord_id);
                        int new_vol = 0;
                        int oldrest = 0;
                        oldrest = oldrec.getRestVolume();
                        new_vol = oldrest - rec.getVolume();
                        if ( new_vol > 0 ) {
                            oldrec.setRestVolume(new_vol);
                            orders_map.put(ord_id, oldrec);
                        } else { //ордер полностью исполнился
                            orders_map.remove(ord_id);
                        }
                        bstate.addForDealType(ord_id, oldrec.getType(), oldrec.getOrderPrice(), rec.getVolume()*(-1)); //уменьшение на объем сделки
                    }
                }

                if ( rec.isCrossTrade() ) {
                    if ( orders_map.containsKey(ord_id) ) {
                        oldrec = orders_map.get(ord_id);
                        bstate.addForDealType(ord_id, oldrec.getType(), oldrec.getOrderPrice(), oldrec.getRestVolume()*(-1));
                    }
                    orders_map.remove(ord_id);
                } else if ( rec.isCounter() ){
                    if (!rec.isAdd() && !rec.isFill() && !rec.isQuote() && !rec.isMoved() && !rec.isCrossTrade() && !rec.isCanceled() && rec.isEndTransaction()) {
                        if ( orders_map.containsKey(ord_id) ) { //ликвидация сделки
                            oldrec = orders_map.get(ord_id);
                            bstate.addForDealType(ord_id, oldrec.getType(), oldrec.getOrderPrice(), (oldrec.getRestVolume() - rec.getRestVolume())*(-1));
                        }
                    }
                }

            } else { //отмена (удаление) заявки
                if ( orders_map.containsKey(ord_id) ){
                    oldrec = orders_map.get(ord_id);
                    bstate.addForDealType(ord_id, oldrec.getType(), oldrec.getOrderPrice(), oldrec.getRestVolume()*(-1));
                    orders_map.remove(ord_id);
                }
            }

        }
    }


    public void bookImportTest(String fileName, char delim, String symb_code){
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

    //стакан на определенное время
    public void printBookOnTime(Iterator<OrdersLogRecord>  qreader, int book_size, Calendar cal, int hour, int minute, int sec){
        //cal.get(Calendar.mi)
        while ( qreader.hasNext()  ) {
            OrdersLogRecord rec = qreader.next();
            processOrderRecord(rec);
            cal.setTimeInMillis(rec.getTime().getTime());
            if ( rec.isEndTransaction() && cal.get(Calendar.HOUR_OF_DAY) >= hour && cal.get(Calendar.MINUTE) >= minute && cal.get(Calendar.SECOND) >= sec  ) {
                this.printBookState(book_size);
                break;
            }
        }
    }

}
