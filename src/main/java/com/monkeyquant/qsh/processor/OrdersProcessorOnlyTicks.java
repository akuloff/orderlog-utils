package com.monkeyquant.qsh.processor;

import com.alex09x.qsh.reader.type.OrdersLogRecord;
import com.monkeyquant.jte.primitives.history.HistoryTick;
import com.monkeyquant.jte.primitives.model.TradeInstrument;
import com.monkeyquant.qsh.Utils;
import com.monkeyquant.qsh.model.IMarketActionListener;
import com.monkeyquant.qsh.model.TickDataEvent;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;

/**
 * передает только событие onNewTick, не содержит bookState
 */

@Slf4j
public class OrdersProcessorOnlyTicks extends AbstractOrdersProcessorWithListener {

    public OrdersProcessorOnlyTicks(IMarketActionListener marketActionListener) {
        super(marketActionListener);
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
                        marketActionListener.onNewTick(TickDataEvent.builder().tickData(tickData).time(Instant.ofEpochMilli(rec.getTime().getTime())).build());
                    } catch (Exception e) {
                        log.warn("onNewTick error", e);
                    }
                    lastDealId = rec.getDealId();
                }
            }
        }
    }

}
