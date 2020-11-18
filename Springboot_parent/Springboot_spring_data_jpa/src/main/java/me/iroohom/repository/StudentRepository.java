package me.iroohom.repository;

import me.iroohom.pojo.Student;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/**
 * @ClassName: StudentRepository
 * @Author: Roohom
 * @Function: StudentRepository继承JpaRepository 完成对数据库的操作
 * @Date: 2020/10/14 14:59
 * @Software: IntelliJ IDEA
 */
public interface StudentRepository extends JpaRepository<Student,Integer>{
    // 需求1: 根据姓名和性别查询用户信息
    public List<Student> findByNameAndSex(String name,String sex);

    // 需求2: 根据姓名或性别查询用户信息
    public List<Student> findByNameOrSex(String name,String sex);

    // 需求3: 查询姓张的学生
    public List<Student> findByNameStartingWith(String name);
    public List<Student> findByNameLike(String name);

    // 需求4: 根据多个id查询用户信息
    public List<Student> findBySid(List<Integer> ids);

}
