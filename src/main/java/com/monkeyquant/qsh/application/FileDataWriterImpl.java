package com.monkeyquant.qsh.application;

import com.monkeyquant.qsh.model.IDataWriter;
import lombok.Setter;

import java.io.FileWriter;

public class FileDataWriterImpl implements IDataWriter {

  @Setter
  private FileWriter fileWriter;

  public FileDataWriterImpl(FileWriter fileWriter) {
    this.fileWriter = fileWriter;
  }

  @Override
  public void write(String s) throws Exception{
    fileWriter.write(s);
  }
}
