package com.monkeyquant.qsh.processor;

import com.alex09x.qsh.reader.type.DealType;
import com.alex09x.qsh.reader.type.OrdersLogRecord;
import lombok.Getter;
import lombok.Setter;

public abstract class OrdersProcessorDebugStats extends OrdersProcessorBookMap {
    @Getter
    @Setter
    private java.sql.Timestamp beginTime, endTime;

    //TODO dataclass

    //сбор статистики для отладки
    int total_recs =0, unk_count = 0;
    int add_cnt = 0, fill_cnt = 0, cancel_cnt = 0, quote_cnt = 0, move_cnt = 0, nonsys_cnt = 0, buy_cnt = 0, sell_cnt = 0;
    int total_fill = 0, total_end_fill = 0, fill_no_order = 0, fill_novolume = 0, fill_notfound = 0, fill_notmatch = 0;
    int noprice_cnt = 0, cross_cnt = 0, fill_noprice = 0, cancel_notfound = 0, only_move_cnt = 0, cross_notfound = 0;
    int move_add = 0, move_fill = 0, move_notfound = 0, move_price = 0, move_volume = 0, move_type = 0;
    int newmove_price = 0, newmove_value = 0, move_remove = 0, nodeal_price = 0, move_exists = 0, end_trans = 0;
    int counter_cnt = 0, counter_only = 0, counter_end = 0;


    //вывод статистики (для отладки)
    public String getStatsInfo(){
        return "total_recs = " + total_recs + " |unk_count: " + unk_count + " |add_cnt: " + add_cnt + " |fill_cnt: " + fill_cnt + " |cancel_cnt: " + cancel_cnt + " |quote_cnt: " + quote_cnt + " |move_cnt: " + move_cnt + " |nonsys_cnt: " + nonsys_cnt + "\n" +
        "buy_cnt: " + buy_cnt + " |sell_cnt: " + sell_cnt + " |buy+sell: " + (buy_cnt + sell_cnt) + "\n" +
        "total_fill: " + total_fill +  " |total_end_fill: " + total_end_fill + "\n" +
        "noprice_cnt: " + noprice_cnt + " |fill_no_order: " + fill_no_order + " |fill_novolume: " + fill_novolume + " |fill_notfound: " + fill_notfound + " |fill_notmatch: " + fill_notmatch + "\n" +
        "cross_cnt: " + cross_cnt + " |fill_noprice: " + fill_noprice + " |cancel_notfound: " + cancel_notfound + " |cross_notfound: " + cross_notfound + "\n" +
        "only_move_cnt: " + only_move_cnt + " |move_add: " + move_add + " |move_fill: " + move_fill + "\n" +
        "move_notfound: " + move_notfound + " |move_price: " + move_price + " |move_volume: " + move_volume + " |move_type: " + move_type + " |move_exists: " + move_exists + "\n" +
        "newmove_price: " + newmove_price + " |newmove_value: " + newmove_value + " |move_remove: " + move_remove + " |nodeal_price: " + nodeal_price + " |end_trans: " + end_trans + "\n" +
        "counter_cnt: " + counter_cnt + " |counter_only: " + counter_only + " |counter_end: " + counter_end;
    }

    @Override
    public void processOrderRecord(OrdersLogRecord rec) {
        processOrderRecordStats(rec, beginTime, endTime);
    }

    //со сбором статистики (для отладки)
    private boolean processOrderRecordStats(OrdersLogRecord rec, java.sql.Timestamp bTime, java.sql.Timestamp eTime){
        OrdersLogRecord oldrec;
        boolean rval = false;

        if ( (bTime == null || rec.getTime().after(bTime)) && (eTime == null || rec.getTime().before(eTime)) ) {
            total_recs ++;
            rval = true;

            if ( rec.isNewSession() ) {
                ordersMap.clear();
                bstate.clearAll();
            }

            long ord_id = rec.getOrderId();

            if (ord_id > 0) {
                if (!rec.isCanceled()) {
                    if (rec.isMoved()) {
                        move_cnt++;
                        if (ordersMap.containsKey(ord_id)) { //заяка уже была добавлена
                            oldrec = ordersMap.get(ord_id);
                            if (rec.getRestVolume() == 0) { //объема в заявке не осталось (значит пеернос заявки - то же что и удаление и постановка новой)
                                ordersMap.remove(ord_id);
                                move_remove++;
                                bstate.addForDealType(rec.getTime(), oldrec.getType(), oldrec.getOrderPrice(), oldrec.getRestVolume() * (-1));
                            } else {
                                bstate.addForDealType(rec.getTime(), oldrec.getType(), oldrec.getOrderPrice(), (oldrec.getRestVolume() - rec.getRestVolume()) * (-1));
                            }
                            move_exists++;
                        }
                    }

                    if (rec.isAdd() && !rec.isNonSystem()) { //добавление новой заявки
                        if (!ordersMap.containsKey(ord_id)) {
                            ordersMap.put(ord_id, rec);
                            bstate.addForDealType(rec.getTime(), rec.getType(), rec.getOrderPrice(), rec.getRestVolume());
                        }
                    } else if (rec.isFill()) { //сделка по заявке
                        total_fill++;
                        if (rec.getRestVolume() == 0) total_end_fill++;

                        if (ordersMap.containsKey(ord_id)) {
                            oldrec = ordersMap.get(ord_id);
                            if (rec.getRestVolume() > oldrec.getVolume()) {
                                System.out.println("!rest is bigger than initial volume: " + ord_id);
                            }

                            int new_vol = 0;
                            int oldrest = 0;

                            oldrest = oldrec.getRestVolume();
                            new_vol = oldrest - rec.getVolume();

                            if (new_vol > 0) {
                                oldrec.setRestVolume(new_vol);
                                ordersMap.put(ord_id, oldrec);
                            } else { //ордер полностью исполнился
                                ordersMap.remove(ord_id);
                            }

                            if (new_vol != rec.getRestVolume()) {
                                //System.out.println("fill, new vol and rest not match, ord: " + ord_id + " |new_vol: " + new_vol + " |new.rest: " + rec.getRestVolume() + " |rec_vol: " + rec.getVolume() + " |old.rest: " + oldrest);
                                fill_notmatch++;
                            }

                            bstate.addForDealType(rec.getTime(), oldrec.getType(), oldrec.getOrderPrice(), rec.getVolume() * (-1));

                        } else {
                            fill_notfound++;
                        }

                    }

                } else if (rec.isCanceled()) { //отмена (удаление) заявки
                    if (ordersMap.containsKey(ord_id)) {
                        oldrec = ordersMap.get(ord_id);
                        bstate.addForDealType(rec.getTime(), oldrec.getType(), oldrec.getOrderPrice(), oldrec.getRestVolume() * (-1));
                        ordersMap.remove(ord_id);
                    } else {
                        //System.out.println("!!! can not find order for remove: " + ord_id);
                        cancel_notfound++;
                    }

                    cancel_cnt++;
                }

                if (rec.isCrossTrade()) {
                    cross_cnt++;
                    //System.out.println("cross, ord: " + ord_id + " |is_add: " + rec.isAdd() + " |is_cancel: " + rec.isCanceled() + " |is_fill: " + rec.isFill());
                    if (!ordersMap.containsKey(ord_id)) {
                        cross_notfound++;
                    } else {
                        oldrec = ordersMap.get(ord_id);
                        bstate.addForDealType(rec.getTime(), oldrec.getType(), oldrec.getOrderPrice(), oldrec.getRestVolume() * (-1));
                    }

                    ordersMap.remove(ord_id);
                }


                if (rec.isCounter()) {
                    counter_cnt++;
                    if (!rec.isAdd() && !rec.isFill() && !rec.isQuote() && !rec.isMoved() && !rec.isCrossTrade() && !rec.isCanceled()) {
                        counter_only++;

                        if (rec.isEndTransaction()) {
                            counter_end++;
                            if (ordersMap.containsKey(ord_id)) { //ликвидация сделки
                                oldrec = ordersMap.get(ord_id);
                                bstate.addForDealType(rec.getTime(), oldrec.getType(), oldrec.getOrderPrice(), (oldrec.getRestVolume() - rec.getRestVolume()) * (-1));
                            }
                        }
                    }
                }

            }

            if (rec.isFill() && ((rec.getHeaderFlags() & 4) == 0)) {
                noprice_cnt++;
            }

            if (rec.isAdd()) add_cnt++;
            if (rec.isFill()) fill_cnt++;
            if (rec.isQuote()) quote_cnt++;

            if (rec.isNonSystem()) nonsys_cnt++;

            if (rec.getType() == DealType.BUY) {
                buy_cnt++;
            } else if (rec.getType() == DealType.SELL) {
                sell_cnt++;
            } else if (rec.getType() == DealType.UNKNOWN) {
                unk_count++;
            }

            if (rec.getDealPrice() == 0) nodeal_price++;
            if (rec.isEndTransaction()) end_trans++;
        }

        return rval;
    }

}
