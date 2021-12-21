package me.roohom;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import me.roohom.entity.Student;
import me.roohom.mapper.StudentMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.List;

@SpringBootTest
public class ApplicationTest {

    @Resource
    private StudentMapper studentMapper;

    @Test
    public void testSelect() {
        System.out.println(("----- selectAll method test ------"));
        QueryWrapper<Student> queryWrapper = new QueryWrapper<>();
        queryWrapper.ge(true, "id", 2)
                .eq(true, "id", 2);

        List<Student> userList = studentMapper.selectList(queryWrapper);
        userList.forEach(System.out::println);

        UpdateWrapper<Student> studentUpdateWrapper = new UpdateWrapper<>();
        studentUpdateWrapper.eq(true, "id", 2);
        Student student = new Student();
        student.setId(2);
        student.setName("jack");
        student.setSex("male");
        int update = studentMapper.update(student, studentUpdateWrapper);
        System.out.println(update);

    }

}
