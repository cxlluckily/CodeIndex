package me.iroohom.repository;

import me.iroohom.pojo.Student;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Optional;

/**
 * @ClassName: StudentRepositoryTest
 * @Author: Roohom
 * @Function: 测试类
 * @Date: 2020/10/14 15:23
 * @Software: IntelliJ IDEA
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class StudentRepositoryTest {

    @Autowired
    private StudentRepository studentRepository;

    @Test
    public void studentRepoTest() {
        Optional<Student> student = studentRepository.findById(2);
        System.out.println(student);
    }


    @Test
    public void studentRepoInsert() {
        Student student = new Student();
        student.setSid(2);
        student.setAge(32);
        student.setName("张三丰");
        student.setSex("女");
        Student save = studentRepository.save(student);
        System.out.println(save);
    }


    @Test
    public void studentRepoQueryAll() {
        List<Student> studentList = studentRepository.findAll();
        for (Student student : studentList) {
            System.out.println(student);
        }
    }

    @Test
    public void test03() {
        List<Student> students = studentRepository.findByNameAndSex("马国保", "男");
        System.out.println(students);
    }

    @Test
    public void test04() {
        List<Student> students = studentRepository.findByNameLike("马%");
        System.out.println(students);
    }
}
