package com.monkeyquant.qsh;

import com.alex09x.qsh.reader.type.DealType;

import java.util.Collections;
import java.util.TreeMap;

public class BookState {
    private TreeMap<Double, Integer> buyPositions = new TreeMap<>(Collections.reverseOrder());
    private TreeMap<Double, Integer> sellPositions = new TreeMap<>();

    public void clearAll(){
        buyPositions.clear();
        sellPositions.clear();
    }

    private void addToTreeMap(TreeMap<Double, Integer> map, double price, int value){
        if (map.containsKey(price)){
            int newvol = map.get(price) + value;
            map.put(price, newvol);
        } else {
            map.put(price, value);
        }
    }

    public void addSell(long deal_id, double price, int value){
        addToTreeMap(sellPositions, price, value);
    }

    public void addBuy(long deal_id, double price, int value){
        addToTreeMap(buyPositions, price, value);
    }


    public void addForDealType(long deal_id, DealType dtype, double price, int value){
        if (dtype == DealType.BUY){
            addBuy(deal_id, price, value);
        }else if (dtype == DealType.SELL){
            addSell(deal_id, price, value);
        }
    }


    public TreeMap<Double, Integer> getBuyPositions() {
        return buyPositions;
    }

    public TreeMap<Double, Integer> getSellPositions() {
        return sellPositions;
    }


}
