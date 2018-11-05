package com.monkeyquant.qsh;

import com.alex09x.qsh.reader.type.DealType;
import com.alex09x.qsh.reader.type.OrdersLogRecord;
import com.monkeyquant.qsh.model.*;

import java.sql.Timestamp;
import java.util.HashMap;

public class OrdersProcessorHashMap implements IOrdersProcessor{
    protected final IBookState bstate = new BookState();
    protected final HashMap<Long, OrdersLogRecord> ordersMap = new HashMap<>();
    private long lastDealId = 0;
    private IMarketActionListener marketActionListener = null;

    public OrdersProcessorHashMap(IMarketActionListener marketActionListener) {
        this.marketActionListener = marketActionListener;
    }

    public OrdersProcessorHashMap() {
    }

    public IBookState getBookState() {
        return bstate;
    }


    protected void changeBookState(Timestamp time, DealType dealType, double price, int value){
        bstate.addForDealType(dealType, price, value);
        if (marketActionListener != null) {
            marketActionListener.onBookChange(time);
        }
    }


    public void processOrderRecord(OrdersLogRecord rec){
        OrdersLogRecord oldrec;

        //новая сессия, очистка массивов
        if ( rec.isNewSession() ) {
            ordersMap.clear();
            bstate.clearAll();
            lastDealId = 0;
        }

        long orderId = rec.getOrderId();
        if (orderId > 0) {
            if (!rec.isCanceled()) {
                if (rec.isMoved()) {
                    if (ordersMap.containsKey(orderId)) { //заяка уже была добавлена
                        oldrec = ordersMap.get(orderId);
                        int volume;
                        if (rec.getRestVolume() == 0) { //объема в заявке не осталось (значит пеернос заявки - то же что и удаление и постановка новой)
                            ordersMap.remove(orderId);
                            volume = Math.negateExact(oldrec.getRestVolume());
                        } else {
                            volume = Math.negateExact(oldrec.getRestVolume() - rec.getRestVolume());
                        }
                        changeBookState(rec.getTime(), oldrec.getType(), oldrec.getOrderPrice(), volume);
                    }
                }

                if (rec.isAdd() && !rec.isNonSystem()) { //добавление новой заявки
                    if (!ordersMap.containsKey(orderId)) {
                        ordersMap.put(orderId, rec);
                        changeBookState(rec.getTime(), rec.getType(), rec.getOrderPrice(), rec.getRestVolume());
                    }
                } else if (rec.isFill()) { //сделка по заявке

                    if (ordersMap.containsKey(orderId)) {
                        oldrec = ordersMap.get(orderId);
                        int newVolume, oldrest;
                        oldrest = oldrec.getRestVolume();
                        newVolume = oldrest - rec.getVolume();
                        if (newVolume > 0) { //если ордер закрывает лимитный ордер и остается еще объем
                            oldrec.setRestVolume(newVolume);
                            ordersMap.put(orderId, oldrec);
                        } else { //ордер полностью исполнился
                            ordersMap.remove(orderId);
                        }
                        changeBookState(rec.getTime(), oldrec.getType(), oldrec.getOrderPrice(), Math.negateExact(rec.getVolume()));

                        if (marketActionListener != null) {
                            if(!rec.isCanceled() && !rec.isCrossTrade() && !rec.isMoved() && !rec.isNonSystem() && rec.getDealId() > 0 && !rec.isCounter()
                              && rec.getDealId() != lastDealId
                            ){
                                TickData tickData = TickData.builder()
                                  .dealType(Utils.fromQshTypeReverse(oldrec.getType()))
                                  .price(rec.getOrderPrice())
                                  .volume(rec.getVolume())
                                  .build();
                                marketActionListener.onNewTick(tickData);
                                lastDealId = rec.getDealId();
                            }
                        }

                    }
                }

                if (rec.isCrossTrade()) {
                    if (ordersMap.containsKey(orderId)) {
                        oldrec = ordersMap.get(orderId);
                        changeBookState(rec.getTime(), oldrec.getType(), oldrec.getOrderPrice(), Math.negateExact(oldrec.getRestVolume()));
                    }
                    ordersMap.remove(orderId);
                } else if (rec.isCounter()) {
                    if (!rec.isAdd() && !rec.isFill() && !rec.isQuote() && !rec.isMoved() && !rec.isCrossTrade() && !rec.isCanceled() && rec.isEndTransaction()) {
                        if (ordersMap.containsKey(orderId)) { //ликвидация сделки
                            oldrec = ordersMap.get(orderId);
                            changeBookState(rec.getTime(), oldrec.getType(), oldrec.getOrderPrice(), Math.negateExact(oldrec.getRestVolume() - rec.getRestVolume()));
                        }
                    }
                }

            } else { //отмена (удаление) заявки
                if (ordersMap.containsKey(orderId)) {
                    oldrec = ordersMap.get(orderId);
                    changeBookState(rec.getTime(), oldrec.getType(), oldrec.getOrderPrice(), Math.negateExact(oldrec.getRestVolume()));
                    ordersMap.remove(orderId);
                }
            }


        }
    }

}
