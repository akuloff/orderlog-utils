package com.monkeyquant.qsh.processor;

import com.alex09x.qsh.reader.type.DealType;
import com.alex09x.qsh.reader.type.OrdersLogRecord;
import com.monkeyquant.jte.primitives.history.HistoryTick;
import com.monkeyquant.jte.primitives.interfaces.IBookState;
import com.monkeyquant.jte.primitives.interfaces.ITickData;
import com.monkeyquant.jte.primitives.model.TradeInstrument;
import com.monkeyquant.qsh.Utils;
import com.monkeyquant.qsh.model.BookStateEvent;
import com.monkeyquant.qsh.model.IMarketActionListener;
import com.monkeyquant.qsh.model.MapBookState;
import com.monkeyquant.qsh.model.TickDataEvent;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;

@Slf4j
public class OrdersProcessorBookMap extends AbstractOrdersProcessorWithListener{
    protected final MapBookState bstate = new MapBookState();

    //TODO use alternative fast map - like OpenHFT, Trove, etc
    protected final HashMap<Long, OrdersLogRecord> ordersMap = new HashMap<>();

    private final boolean sendTicks;

    private int maxMapSize = 0;

    public OrdersProcessorBookMap(IMarketActionListener marketActionListener, boolean sendTicks) {
        super(marketActionListener);
        this.sendTicks = sendTicks;
    }

    public OrdersProcessorBookMap(IMarketActionListener marketActionListener) {
        this(marketActionListener, false);
    }

    @Override
    public void end() throws Exception {
        super.end();
        log.debug("max map size: {}, map stats - putCount: {}, setCount: {}, getCount: {}", maxMapSize, bstate.getPutCount(), bstate.getSetCount(), bstate.getGetCount());
    }

    public OrdersProcessorBookMap(boolean sendTicks) {
        super(null);
        this.sendTicks = false;
    }

    public OrdersProcessorBookMap() {
        this(false);
    }

    public IBookState getBookState() {
        return bstate;
    }

    private void changeBookState(OrdersLogRecord rec, Timestamp time, DealType dealType, double price, int value){
        bstate.addForDealType(time, dealType, price, value);
        if (marketActionListener != null && rec.isEndTransaction()) {
            if (instrument == null) {
                instrument = new TradeInstrument(rec.getSymbol());
                bstate.setInstrument(instrument);
            }
            try {
                bstate.setDate(time);
                marketActionListener.onBookChange(BookStateEvent.builder().bookState(bstate).time(time).build());
            } catch (Exception e) {
                System.out.println(e);
            }
        }

        if (ordersMap.size() > maxMapSize) {
            maxMapSize = ordersMap.size();
        }
    }


    public void processOrderRecord(OrdersLogRecord rec){
        OrdersLogRecord oldrec;

        //новая сессия, очистка массивов
        if ( rec.isNewSession() ) {
            ordersMap.clear();
            bstate.clearAll();
            lastDealId = 0;
            instrument = null;
        }

        long orderId = rec.getOrderId();
        if (orderId > 0) {
            if (!rec.isCanceled()) {
                if (rec.isMoved()) {
                    if (ordersMap.containsKey(orderId)) { //заяка уже была добавлена
                        oldrec = ordersMap.get(orderId);
                        int volume;
                        if (rec.getRestVolume() == 0) { //объема в заявке не осталось (значит перенос заявки - то же что и удаление и постановка новой)
                            ordersMap.remove(orderId);
                            volume = Math.negateExact(oldrec.getRestVolume());
                        } else {
                            volume = Math.negateExact(oldrec.getRestVolume() - rec.getRestVolume());
                        }
                        changeBookState(rec, rec.getTime(), oldrec.getType(), oldrec.getOrderPrice(), volume);
                    }
                }

                if (rec.isAdd() && !rec.isNonSystem()) { //добавление новой заявки
                    if (!ordersMap.containsKey(orderId)) {
                        ordersMap.put(orderId, rec);
                        changeBookState(rec, rec.getTime(), rec.getType(), rec.getOrderPrice(), rec.getRestVolume());
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
                        changeBookState(rec, rec.getTime(), oldrec.getType(), oldrec.getOrderPrice(), Math.negateExact(rec.getVolume()));

                        if (marketActionListener != null && sendTicks) {
                            if (!rec.isCanceled() && !rec.isCrossTrade() && !rec.isMoved() && !rec.isNonSystem() && rec.getDealId() > 0 && !rec.isCounter()
                              && rec.getDealId() != lastDealId && rec.getDealPrice() > 0 && rec.isEndTransaction()
                            ) {
                                ITickData tickData = HistoryTick.builder()
                                  .instrument(instrument)
                                  .buyFlag(Utils.buyFlagFromDealType(Utils.fromQshTypeReverse(oldrec.getType())))
                                  .price(rec.getDealPrice())
                                  .amount(rec.getVolume())
                                  .date(rec.getTime())
                                  .tradeId(String.valueOf(rec.getDealId()))
                                  .build();

                                try {
                                    marketActionListener.onNewTick(TickDataEvent.builder().time(Instant.ofEpochMilli(rec.getTime().getTime())).tickData(tickData).build());
                                } catch (Exception e) {
                                    System.out.println(e);
                                }
                                lastDealId = rec.getDealId();
                            }
                        }

                    }
                }

                if (rec.isCrossTrade()) {
                    if (ordersMap.containsKey(orderId)) {
                        oldrec = ordersMap.get(orderId);
                        changeBookState(rec, rec.getTime(), oldrec.getType(), oldrec.getOrderPrice(), Math.negateExact(oldrec.getRestVolume()));
                        ordersMap.remove(orderId);
                    }
                } else if (rec.isCounter()) {
                    if (!rec.isAdd() && !rec.isFill() && !rec.isQuote() && !rec.isMoved() && !rec.isCrossTrade() && !rec.isCanceled() && rec.isEndTransaction()) {
                        if (ordersMap.containsKey(orderId)) { //ликвидация сделки
                            oldrec = ordersMap.get(orderId);
                            changeBookState(rec, rec.getTime(), oldrec.getType(), oldrec.getOrderPrice(), Math.negateExact(oldrec.getRestVolume() - rec.getRestVolume()));
                        }
                    }
                }

            } else { //отмена (удаление) заявки
                if (ordersMap.containsKey(orderId)) {
                    oldrec = ordersMap.get(orderId);
                    changeBookState(rec, rec.getTime(), oldrec.getType(), oldrec.getOrderPrice(), Math.negateExact(oldrec.getRestVolume()));
                    ordersMap.remove(orderId);
                }
            }

        }
    }

}
