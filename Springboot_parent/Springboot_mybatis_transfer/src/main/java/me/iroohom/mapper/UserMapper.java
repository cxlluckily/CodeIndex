package me.iroohom.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Repository;

/**
 * @ClassName: UserMapper
 * @Author: Roohom
 * @Function: 定义功能的UserMapper类
 * @Date: 2020/10/14 18:56
 * @Software: IntelliJ IDEA
 */

@Mapper
@Repository
public interface UserMapper {

    /**
     * 转账转出操作
     *
     * @param name  出账用户名
     * @param money 出账额
     */
    @Update("update tb_account set money = money + #{money} where name = #{name}")
    void inMoney(@Param("name") String name, @Param("money") Double money);

    /**
     * 转账入账操作
     *
     * @param name  入账用户名
     * @param money 入账额
     */
    @Update("update tb_account set money = money - #{money} where name = #{name}")
    void outMoney(@Param("name") String name, @Param("money") Double money);

    /**
     * 查询指定用户名的用户的账户余额
     *
     * @param name 用户账户
     * @return 账户余额
     */
    @Select("select money from tb_account where name = #{name}")
    Integer queryMoney(@Param("name") String name);

    /**
     * 根据用户名查询数据库中是否存在该用户
     *
     * @param name 账户名
     * @return 账户名 | null
     */
    @Select("select name from tb_account where name = #{name}")
    String queryName(@Param("name") String name);


}
