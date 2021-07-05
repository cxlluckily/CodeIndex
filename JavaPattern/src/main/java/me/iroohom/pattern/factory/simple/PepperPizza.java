package me.iroohom.pattern.factory.simple;

public class PepperPizza extends Pizza {
    @Override
    public void prepare() {
        System.out.println("辣椒披萨");
    }
}
