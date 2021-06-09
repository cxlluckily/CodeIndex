package me.iroohom.pattern.singleton;

/**
 * 饿汉式 HUNGRY
 * 静态方法最先加载，当加载完成之后调用内部静态对象(实例化一个Bank对象)
 * 当实例化成功之后，由于不能调用Bank的实例化方法直接new一个对象，只能
 * 使用提供的公共的静态方法去取已经实例化好的静态对象，所以每次调用Bank得到的都是
 * 同一个预先加载好的静态对象
 * 坏处：对象加载之后可能一直用不到，创建时间过长
 * 好处：是线程安全的
 *
 * @author roohom
 */
public class Bank {
    /**
     * 私有化类的构造器
     */
    private Bank() {

    }

    /**
     * 内部类创建的对象，要求此对象和静态的方法一致也是静态的
     */
    private static Bank bankInstance = new Bank();


    /**
     * 提供公共静态的方法，返回类的对象
     *
     * @return 类的对象
     */
    public static Bank getInstance() {
        return bankInstance;
    }
}
