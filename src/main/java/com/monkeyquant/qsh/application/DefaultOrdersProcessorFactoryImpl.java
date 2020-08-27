package com.monkeyquant.qsh.application;

import com.monkeyquant.qsh.listeners.BarsCollectorActionListener;
import com.monkeyquant.qsh.listeners.BookStateWriterActionListener;
import com.monkeyquant.qsh.listeners.TicksAndBookStateActionListener;
import com.monkeyquant.qsh.listeners.TicksWriterActionListener;
import com.monkeyquant.qsh.model.IDataWriter;
import com.monkeyquant.qsh.model.IOrdersProcessor;
import com.monkeyquant.qsh.model.IOrdersProcessorFactory;
import com.monkeyquant.qsh.model.OutputFormatType;
import com.monkeyquant.qsh.processor.OrdersProcessorBookMap;
import com.monkeyquant.qsh.processor.OrdersProcessorOnlyTicks;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DefaultOrdersProcessorFactoryImpl implements IOrdersProcessorFactory {
  @Override
  public IOrdersProcessor getOrdersProcessor(IDataWriter writer, ConverterParameters converterParameters) {
    OutputFormatType type = converterParameters.getOutputFormatType();
    if (OutputFormatType.TICKS.equals(type)) {
      return new OrdersProcessorOnlyTicks(new TicksWriterActionListener(writer, converterParameters));
    } else if (OutputFormatType.BOOKSTATE.equals(type)) {
      return new OrdersProcessorBookMap(new BookStateWriterActionListener(writer, converterParameters));
    } else if (OutputFormatType.BARS.equals(type)) {
      return new OrdersProcessorBookMap(new BarsCollectorActionListener(writer, converterParameters), true);
    } else if (OutputFormatType.BOOK_AND_TICKS.equals(type)) {
      return new OrdersProcessorBookMap(new TicksAndBookStateActionListener(writer, converterParameters), true);
    }
    throw new IllegalArgumentException(String.format("unknown export type: %s", type.name()));
  }
}
