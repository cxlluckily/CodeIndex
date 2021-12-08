package me.roohom;

import lombok.SneakyThrows;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.ArrayList;
import java.util.List;

public class BatchApplication {
    @SneakyThrows
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> strList = new ArrayList<>();
        strList.add("1");
        strList.add("2");
        strList.add("3");
        strList.add("4");
        DataSource<String> strStream = env.fromCollection(strList);
        strStream.print();

        env.execute();
    }
}
