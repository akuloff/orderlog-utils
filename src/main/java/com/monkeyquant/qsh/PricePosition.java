package com.monkeyquant.qsh;

public class PricePosition implements Comparable{
    private double price, volume;

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public int compareTo(Object o) {
        int rval = 0;
        if (o.getClass().equals(this.getClass())){
            if ( ((PricePosition)o).getPrice() > this.price ) {
                rval = -1;
            } else if (((PricePosition)o).getPrice() > this.price){
                rval = 1;
            }
        } else {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        return rval;
    }

}
