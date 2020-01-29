package com.monkeyquant.qsh.application;

import com.monkeyquant.qsh.model.IDataWriter;

import java.io.FileWriter;

public class FileDataWriterImpl implements IDataWriter {
  private final FileWriter fileWriter;

  public FileDataWriterImpl(FileWriter fileWriter) {
    this.fileWriter = fileWriter;
  }

  @Override
  public void write(String s) throws Exception{
    fileWriter.write(s);
  }
}
