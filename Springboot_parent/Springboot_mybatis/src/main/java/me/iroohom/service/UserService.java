package me.iroohom.service;

import me.iroohom.pojo.User;

import java.util.List;

/**
 * @ClassName: UserService
 * @Author: Roohom
 * @Function: service功能接口
 * @Date: 2020/10/14 10:15
 * @Software: IntelliJ IDEA
 */
public interface UserService {

    List<User> findAll();

    User findUserById(Integer id);

}
