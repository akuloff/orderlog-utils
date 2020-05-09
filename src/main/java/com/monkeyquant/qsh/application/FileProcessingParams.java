package com.monkeyquant.qsh.application;

import com.monkeyquant.qsh.model.IOrdersProcessor;
import lombok.Builder;
import lombok.Getter;

import java.io.FileWriter;

@Getter
@Builder
public class FileProcessingParams {
  private String outFileName;
  private FileWriter writer;
  private IOrdersProcessor ordersProcessor;
}
