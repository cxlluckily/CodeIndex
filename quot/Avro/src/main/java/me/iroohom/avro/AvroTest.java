package me.iroohom.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import javax.xml.crypto.Data;
import java.io.File;
import java.io.IOException;

/**
 * @ClassName: AvroTest
 * @Author: Roohom
 * @Function: 演示Avro序列化与反序列化
 * @Date: 2020/10/28 15:43
 * @Software: IntelliJ IDEA
 */
public class AvroTest {
    public static void main(String[] args) throws IOException {

        //新建对象
        User user = new User();

        //封装数据
        user.setName("Allen");
        user.setAge(22);
        user.setAddress("安徽");

        User anoUser = new User("xx", 19, "北京");
        User thirdUser = User.newBuilder()
                .setName("aa")
                .setAge(20)
                .setAddress("西安")
                .build();

        //序列化 定义模式
//        SpecificDatumWriter<Object> datumWriter = new SpecificDatumWriter<>(user.getSchema());
//
//        //数据序列化对象
//        DataFileWriter<Object> dataFileWriter = new DataFileWriter<>(datumWriter);
//
//        //设置序列化文件路径
//        dataFileWriter.create(user.getSchema(),new File("avro.txt"));
//
//        dataFileWriter.append(user);
//        dataFileWriter.append(anoUser);
//        dataFileWriter.append(thirdUser);
//
//        dataFileWriter.close();

        //反序列化
        SpecificDatumReader<User> datumReader = new SpecificDatumReader<>(User.class);
        DataFileReader dataFileReader = new DataFileReader<User>(new File("avro.txt"), datumReader);
        for (Object users : dataFileReader) {
            System.out.println("反序列化" + users);
        }

    }
}
