package me.iroohom.pattern.factory.simple;

public class GreekPizza extends Pizza {
    @Override
    public void prepare() {
        System.out.println("希腊披萨");
    }
}
