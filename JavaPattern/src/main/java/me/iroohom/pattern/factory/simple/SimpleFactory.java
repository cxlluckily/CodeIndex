package me.iroohom.pattern.factory.simple;

/**
 * 简单工厂类
 */
public class SimpleFactory {

    public Pizza createPizza(String orderType) {

        Pizza pizza = null;

        if (orderType.equals("greek")) {
            pizza = new GreekPizza();
            pizza.setName("希腊");
        } else if (orderType.equals("cheese")) {
            pizza = new CheesePizza();
            pizza.setName("奶酪");
        } else if (orderType.equals("pepper")) {
            pizza = new PepperPizza();
            pizza.setName("辣椒");
        }

        return pizza;
    }

}
