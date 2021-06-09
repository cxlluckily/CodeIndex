package me.iroohom.pattern.singleton;

/**
 * @ClassName: SingletonModel
 * @Author: Roohom
 * @Function:
 * @Date: 2021/6/9 23:57
 * @Software: IntelliJ IDEA
 */
public class SingletonModel {
    public static void main(String[] args) {
        Bank bankInstance = Bank.getInstance();
        Bank bankInstance2 = Bank.getInstance();

        System.out.println(bankInstance == bankInstance2);


    }
}
