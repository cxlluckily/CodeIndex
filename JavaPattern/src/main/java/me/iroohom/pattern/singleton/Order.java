package me.iroohom.pattern.singleton;

/**
 * LAZY
 * 好处：延迟对象的创建，用到的时候才创建
 * 坏处：下面的写法是线程不安全的
 * 改进一下 https://zhuanlan.zhihu.com/p/52316864
 *
 * @Author: roohom
 */
public class Order {
    /**
     * 私有化类的构造器
     */
    private Order() {
    }

    /**
     * 声明类的对象 不初始化
     */
    private static Order orderInstance = null;

    /**
     * 声明public、static返回当前类的一个对象
     *
     * @return orderInstance
     */
    public static Order getOrderInstance() {

        //如果orderInstance为null，那说明是第一次调用，就直接new一个
        //线程不安全就体现在这里
        if (orderInstance == null) {
            orderInstance = new Order();
        }
        //如果不是第一次调用，说明之前new过了，那就直接返回之前new过的那个
        return orderInstance;
    }
}
