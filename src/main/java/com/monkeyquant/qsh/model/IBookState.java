package com.monkeyquant.qsh.model;

import com.alex09x.qsh.reader.type.DealType;

import java.util.List;

public interface IBookState {
  void clearAll();
  void addForDealType(DealType dtype, double price, int value);
  List<PriceRecord> getAskPositions(int total);
  List<PriceRecord> getBidPositions(int total);
  List<PriceRecord> getAskPositionsForVolume(int needVolume);
  List<PriceRecord> getBidPositionsForVolume(int needVolume);
  long getPutCount();
  long getSetCount();
  long getRemoveCount();
  int getSellSize();
  int getBuySize();
}
