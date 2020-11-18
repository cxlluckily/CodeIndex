package me.iroohom.pojo;


import lombok.Data;

/**
 * @ClassName: User
 * @Author: Roohom
 * @Function: 普通Java bean 用于存储用户数据
 * @Date: 2020/10/14 09:32
 * @Software: IntelliJ IDEA
 */
@Data
public class User {
    private Integer id;
    private String username;
    private String password;
    private String name;
}
