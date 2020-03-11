package com.monkeyquant.qsh.processor;

import com.monkeyquant.jte.primitives.model.TradeInstrument;
import com.monkeyquant.qsh.model.IMarketActionListener;
import com.monkeyquant.qsh.model.IOrdersProcessor;

public abstract class AbstractOrdersProcessorWithListener implements IOrdersProcessor {
  protected final IMarketActionListener marketActionListener;
  protected long lastDealId = 0;
  protected TradeInstrument instrument = null;

  public AbstractOrdersProcessorWithListener(IMarketActionListener marketActionListener) {
    this.marketActionListener = marketActionListener;
  }

  @Override
  public void init() throws Exception {
    if (marketActionListener != null) {
      marketActionListener.init();
    }
  }

  @Override
  public void end() throws Exception {
    if (marketActionListener != null) {
      marketActionListener.end();
    }
  }
}
