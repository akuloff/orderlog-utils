package com.monkeyquant.qsh.model;

import com.alex09x.qsh.reader.type.DealType;

import java.util.*;

public class BookState implements IBookState{
    private final TreeMap<Double, Integer> buyPositions = new TreeMap<>(Collections.reverseOrder());
    private final TreeMap<Double, Integer> sellPositions = new TreeMap<>();

    protected long putCount = 0;
    protected long setCount = 0;
    protected long removeCount = 0;

    @Override
    public int getSellSize() {
        return sellPositions.size();
    }

    @Override
    public int getBuySize() {
        return buyPositions.size();
    }

    @Override
    public long getPutCount() {
        return putCount;
    }

    @Override
    public long getSetCount() {
        return setCount;
    }

    @Override
    public long getRemoveCount() {
        return removeCount;
    }

    public void clearAll(){
        buyPositions.clear();
        sellPositions.clear();
    }

    private void addToTreeMap(TreeMap<Double, Integer> map, double price, int value){
        Integer mapValue;
        mapValue = map.get(price);
        int newValue;
        if (mapValue != null) {
            newValue = mapValue + value;
//            if (newValue == 0) {
//                removeCount ++;
//                map.remove(price);
//            } else {
//                setCount ++;
//                map.replace(price, newValue);
//            }
            setCount ++;
            map.replace(price, newValue);
        } else {
            putCount ++;
            map.put(price, value);
        }
    }

    @Override
    public void addForDealType(DealType dtype, double price, int value){
        if (dtype == DealType.BUY){
            addToTreeMap(buyPositions, price, value);
        }else if (dtype == DealType.SELL){
            addToTreeMap(sellPositions, price, value);
        }
    }

    private List<PriceRecord> getMapRecords(Map<Double, Integer> map, int total){
        ArrayList<PriceRecord> records = new ArrayList<>();
        int cnt = 0;
        for(Map.Entry<Double, Integer> e: map.entrySet()){
            if (e.getValue() > 0) {
                records.add(PriceRecord.builder()
                  .price(e.getKey())
                  .value(e.getValue())
                  .build());
                cnt++;
                if (cnt >= total) break;
            }
        }
        return records;
    }

    @Override
    public List<PriceRecord> getAskPositions(int total) {
        return getMapRecords(this.sellPositions, total);
    }

    @Override
    public List<PriceRecord> getBidPositions(int total) {
        return getMapRecords(this.buyPositions, total);
    }

    @Override
    public List<PriceRecord> getAskPositionsForVolume(int needVolume) {
        return null;
    }

    @Override
    public List<PriceRecord> getBidPositionsForVolume(int needVolume) {
        return null;
    }
}
