package me.iroohom.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName: User
 * @Author: Roohom
 * @Function: User Entity
 * @Date: 2020/10/14 16:35
 * @Software: IntelliJ IDEA
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {
    private String name;
    private String password;
}
