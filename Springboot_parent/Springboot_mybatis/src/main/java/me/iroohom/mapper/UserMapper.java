package me.iroohom.mapper;

import me.iroohom.pojo.User;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @ClassName: Usermapper
 * @Author: Roohom
 * @Function: 定义功能的Mapper类
 * @Date: 2020/10/14 09:39
 * @Software: IntelliJ IDEA
 */
@Mapper
/**
 * 将当前类放入springboot容器（可加可不加）
 */
@Repository
public interface UserMapper {

    List<User> queryAll();

    User queryUserById(Integer id);

}
