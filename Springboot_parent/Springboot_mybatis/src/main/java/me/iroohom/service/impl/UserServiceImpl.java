package me.iroohom.service.impl;

import me.iroohom.mapper.UserMapper;
import me.iroohom.pojo.User;
import me.iroohom.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @ClassName: UserServiceImpl
 * @Author: Roohom
 * @Function: UserService实现类
 * @Date: 2020/10/14 10:16
 * @Software: IntelliJ IDEA
 */

@Service
public class UserServiceImpl implements UserService {
    @Autowired
    private UserMapper userMapper;

    @Override
    public List<User> findAll() {
        return userMapper.queryAll();
    }

    @Override
    public User findUserById(Integer id) {
        return userMapper.queryUserById(id);
    }
}
