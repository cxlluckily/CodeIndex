package me.roohom.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.*;

@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
@TableName("stu")
public class Student {
    private int id;
    private String name;
    private String sex;
}
