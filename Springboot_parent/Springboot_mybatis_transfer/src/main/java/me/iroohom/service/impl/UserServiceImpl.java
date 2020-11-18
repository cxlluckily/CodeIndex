package me.iroohom.service.impl;

import me.iroohom.mapper.UserMapper;
import me.iroohom.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * @ClassName: UserServiceImpl
 * @Author: Roohom
 * @Function: Service实现类
 * @Date: 2020/10/14 19:14
 * @Software: IntelliJ IDEA
 */
@Service
public class UserServiceImpl implements UserService {

    @Autowired
    private UserMapper userMapper;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void transfer(String outName, String inName, Double money) throws Exception {
        if (userMapper.queryName(outName) != null && userMapper.queryMoney(outName) <= 0) {
            throw new Exception("转出账户余额小于0,换张卡试试！");
        }
        if (userMapper.queryName(outName) == null || userMapper.queryName(inName) == null) {
            throw new Exception("请再次确认您或者对方账户名!");
        }
        userMapper.outMoney(outName, money);
        userMapper.inMoney(inName, money);
    }
}
