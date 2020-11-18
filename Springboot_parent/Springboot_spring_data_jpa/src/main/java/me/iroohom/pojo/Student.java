package me.iroohom.pojo;

import lombok.Data;

import javax.persistence.*;


/**
 * @ClassName: Student
 * @Author: Roohom
 * @Function: 学生类
 * @Date: 2020/10/14 14:52
 * @Software: IntelliJ IDEA
 */
//指定表名
@Table(name = "tb_student")
//告诉JPA这是一个实体类
@Entity
@Data
public class Student {
    //声明为主键
    @Id
    /**
     * 提供自动增长列，数据添加后返回主键ID
     */
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer sid;

    @Column(name = "name", length = 20)
    private String name;

    @Column(name = "sex", length = 5)
    private String sex;

    @Column(name = "age")
    private Integer age;
}
