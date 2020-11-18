package me.iroohom.pojo;

import lombok.Data;

/**
 * @ClassName: User
 * @Author: Roohom
 * @Function: User类，用于存放用户账户余额数据
 * @Date: 2020/10/14 18:54
 * @Software: IntelliJ IDEA
 */
@Data
public class User {
    private Integer id;
    private String name;
    private Double money;
}
