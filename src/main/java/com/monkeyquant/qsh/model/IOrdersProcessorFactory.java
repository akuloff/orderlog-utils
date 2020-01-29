package com.monkeyquant.qsh.model;

import com.monkeyquant.qsh.application.ConverterParameters;

public interface IOrdersProcessorFactory {
  IOrdersProcessor getOrdersProcessor(IDataWriter writer, ConverterParameters converterParameters);
}
