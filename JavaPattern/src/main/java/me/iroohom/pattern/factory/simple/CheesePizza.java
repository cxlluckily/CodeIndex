package me.iroohom.pattern.factory.simple;

public class CheesePizza extends Pizza {
    @Override
    public void prepare() {
        System.out.println("奶酪披萨");
    }
}
