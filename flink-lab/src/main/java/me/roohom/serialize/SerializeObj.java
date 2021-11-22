package me.roohom.serialize;

import lombok.SneakyThrows;
import me.roohom.bean.Person;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.util.ArrayList;

public class SerializeObj {
    @SneakyThrows
    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper();
        Person person = new Person();
        person.setAge(1);
        person.setName("Mike");

        Person person1 = new Person();
        person1.setName("jack");
        person1.setAge(2);

        ArrayList<Person> people = new ArrayList<>();
        people.add(person);
        people.add(person1);
        for (int i = 0; i <= 10000; i++) {
            people.add(
                    new Person("Mike" + i, i)
            );
        }


        ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(new File("data/obj/people.txt"), true));

        for (int i = 0; i < people.size(); i++) {
            objectOutputStream.writeObject(people.get(i));
        }
        objectOutputStream.close();


        //ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(new File("/Users/roohom/Code/IDEAJ/CodeIndex/flink-lab/data/obj/person.txt")));
        //ArrayList<Person> personArrayList = new ArrayList<>();
        //try {
        //    while (true){
        //        Person person2 = (Person) objectInputStream.readObject();
        //        personArrayList.add(person2);
        //    }
        //}
        //catch (Exception e){
        //
        //}
        //
        //personArrayList.forEach(
        //        x -> {
        //            try {
        //                System.out.println(objectMapper.writeValueAsString(x));
        //            } catch (IOException e) {
        //                e.printStackTrace();
        //            }
        //        }
        //);


    }
}
