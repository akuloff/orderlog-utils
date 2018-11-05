package com.monkeyquant.qsh;

import com.alex09x.qsh.reader.type.OrdersLogRecord;
import com.monkeyquant.qsh.model.IMarketActionListener;
import com.monkeyquant.qsh.model.IOrdersProcessor;
import com.monkeyquant.qsh.model.TickData;

/**
 * передает только событие onNewTick, не содержит bookState
 */

public class OrdersProcessorOnlyTicks implements IOrdersProcessor{
    private final IMarketActionListener marketActionListener;
    private long lastDealId = 0;

    public OrdersProcessorOnlyTicks(IMarketActionListener marketActionListener) {
        this.marketActionListener = marketActionListener;
    }

    @Override
    public void processOrderRecord(OrdersLogRecord rec){
        if ( rec.isNewSession() ) { //новая сессия
            lastDealId = 0;
        }

        long orderId = rec.getOrderId();
        if (orderId > 0) {
            if (rec.isFill() && marketActionListener != null) {
                if (!rec.isCanceled() && !rec.isCrossTrade() && !rec.isMoved() && !rec.isNonSystem() && rec.getDealId() > 0 && !rec.isCounter()
                  && rec.getDealId() != lastDealId
                ) {
                    TickData tickData = TickData.builder()
                      .dealType(Utils.fromQshTypeReverse(rec.getType()))
                      .price(rec.getOrderPrice())
                      .volume(rec.getVolume())
                      .time(rec.getTime())
                      .dealId(rec.getDealId())
                      .build();

                    marketActionListener.onNewTick(tickData);
                    lastDealId = rec.getDealId();
                }
            }
        }
    }

}
