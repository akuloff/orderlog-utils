package com.monkeyquant.qsh.processor;

import com.alex09x.qsh.reader.type.OrdersLogRecord;
import com.monkeyquant.jte.primitives.history.HistoryTick;
import com.monkeyquant.jte.primitives.model.TradeInstrument;
import com.monkeyquant.qsh.Utils;
import com.monkeyquant.qsh.model.IMarketActionListener;
import com.monkeyquant.qsh.model.IOrdersProcessor;
import com.monkeyquant.qsh.model.TickDataEvent;

/**
 * передает только событие onNewTick, не содержит bookState
 */

public class OrdersProcessorOnlyTicks implements IOrdersProcessor{
    private final IMarketActionListener marketActionListener;
    private long lastDealId = 0;
    private TradeInstrument instrument = null;

    public OrdersProcessorOnlyTicks(IMarketActionListener marketActionListener) {
        this.marketActionListener = marketActionListener;
    }

    @Override
    public void init() throws Exception {
        marketActionListener.init();
    }

    @Override
    public void processOrderRecord(OrdersLogRecord rec){
        if ( rec.isNewSession() ) { //новая сессия
            lastDealId = 0;
        }

        long orderId = rec.getOrderId();
        if (orderId > 0) {
            if (rec.isFill() && marketActionListener != null) {
                if (!rec.isAdd() && !rec.isCanceled() && !rec.isCrossTrade() && !rec.isMoved() && !rec.isNonSystem() && rec.getDealId() > 0 && !rec.isCounter()
                  && rec.getDealId() != lastDealId && rec.getDealPrice() > 0 && rec.isEndTransaction()
                ) {
                    HistoryTick tickData = HistoryTick.builder()
                      .buyFlag(Utils.buyFlagFromDealType(Utils.fromQshTypeReverse(rec.getType())))
                      .price(rec.getDealPrice())
                      .amount(rec.getVolume())
                      .date(rec.getTime())
                      .tradeId(String.valueOf(rec.getDealId()))
                      .build();
                    if (instrument == null) {
                        instrument = new TradeInstrument(rec.getSymbol());
                    }
                    tickData.setInstrument(instrument);
                    try {
                        marketActionListener.onNewTick(TickDataEvent.builder().tickData(tickData).time(rec.getTime()).build());
                    } catch (Exception e) {
                        System.out.println(e);
                    }
                    lastDealId = rec.getDealId();
                }
            }
        }
    }

}
