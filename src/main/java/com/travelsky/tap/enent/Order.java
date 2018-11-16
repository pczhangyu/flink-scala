package com.travelsky.tap.enent;

/**
 * Created by pczhangyu on 2018/11/13.
 */
public class Order {
    private Long user;
    private String product;
    private int amount;

    public Order() {
    }

    public Order(Long user, String product, int amount) {
        this.user = user;
        this.product = product;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Order{" +
                "user=" + user +
                ", product='" + product + '\'' +
                ", amount=" + amount +
                '}';
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
