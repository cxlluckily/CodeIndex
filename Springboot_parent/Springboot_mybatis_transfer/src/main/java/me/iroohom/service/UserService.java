package me.iroohom.service;

/**
 * @ClassName: UserService
 * @Author: Roohom
 * @Function: 定义功能的Service接口
 * @Date: 2020/10/14 19:13
 * @Software: IntelliJ IDEA
 */
public interface UserService {

    /**
     * 转账操作接口
     *
     * @param outName 转出账户名
     * @param inName  转入账户名
     * @param money   转出款额
     */
    void transfer(String outName, String inName, Double money) throws Exception;

}
