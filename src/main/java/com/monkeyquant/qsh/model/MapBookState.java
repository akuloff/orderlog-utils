package com.monkeyquant.qsh.model;

import com.alex09x.qsh.reader.type.DealType;
import com.monkeyquant.jte.primitives.interfaces.IBookState;
import com.monkeyquant.jte.primitives.model.PriceRecord;
import com.monkeyquant.jte.primitives.model.TradeInstrument;
import lombok.Getter;
import lombok.Setter;

import java.sql.Timestamp;
import java.util.*;

public class MapBookState implements IBookState {
    private final TreeMap<Double, Integer> buyPositions = new TreeMap<>(Collections.reverseOrder());
    private final TreeMap<Double, Integer> sellPositions = new TreeMap<>();

    @Setter
    private Timestamp date;

    @Getter
    private long putCount = 0;

    @Getter
    private long setCount = 0;

    @Getter
    private long removeCount = 0;

    @Getter
    private long getCount = 0;

    @Setter
    private TradeInstrument instrument;

    @Override
    public TradeInstrument getInstrument() {
        return instrument;
    }

    public void clearAll() {
        buyPositions.clear();
        sellPositions.clear();
        putCount = setCount = removeCount = 0;
    }

    private void addToMap(Map<Double, Integer> map, double price, int value){
        Integer mapValue;
        mapValue = map.get(price);
        int newValue;
        if (mapValue != null) {
            newValue = mapValue + value;
            setCount++;
        } else {
            putCount++;
            newValue = value;
        }
        map.put(price, newValue);
    }

    public void addForDealType(Timestamp timestamp, DealType dtype, double price, int value){
        this.date = timestamp;
        switch (dtype) {
            case BUY:
                addToMap(buyPositions, price, value);
                break;
            case SELL:
                addToMap(sellPositions, price, value);
                break;
        }
    }


    private List<PriceRecord> getMapRecordsForCount(Map<Double, Integer> map, int total){
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
        getCount ++;
        return records;
    }

    private List<PriceRecord> getMapRecordsForVolume(Map<Double, Integer> map, int needVolume){
        ArrayList<PriceRecord> records = new ArrayList<>();
        int totalValue = 0;
        for(Map.Entry<Double, Integer> e: map.entrySet()){
            if (e.getValue() > 0) {
                records.add(PriceRecord.builder()
                  .price(e.getKey())
                  .value(e.getValue())
                  .build());
                totalValue += e.getValue();
                if (totalValue >= needVolume) break;
            }
        }
        getCount ++;
        return records;
    }


    @Override
    public List<PriceRecord> getAskPositions(int total) {
        return getMapRecordsForCount(this.sellPositions, total);
    }

    @Override
    public List<PriceRecord> getBidPositions(int total) {
        return getMapRecordsForCount(this.buyPositions, total);
    }

    @Override
    public List<PriceRecord> getAskPositionsForVolume(int needVolume) {
        return getMapRecordsForVolume(this.sellPositions, needVolume);
    }

    @Override
    public List<PriceRecord> getBidPositionsForVolume(int needVolume) {
        return getMapRecordsForVolume(this.buyPositions, needVolume);
    }

    @Override
    public Long getLastUpdateSystemSequence() {
        return null;
    }

    @Override
    public Timestamp getDate() {
        return date;
    }

    @Override
    public PriceRecord getBestAsk() {
        List<PriceRecord> plist = getMapRecordsForCount(this.sellPositions, 1);
        if (plist.size() > 0) {
            return plist.get(0);
        } else {
            return null;
        }
    }

    @Override
    public PriceRecord getBestBid() {
        List<PriceRecord> plist = getMapRecordsForCount(this.buyPositions, 1);
        if (plist.size() > 0) {
            return plist.get(0);
        } else {
            return null;
        }
    }

    @Override
    public int getBookSize() {
        return Math.min(buyPositions.size(), sellPositions.size());
    }

    @Override
    public String getCustomField() {
        return null;
    }
}
