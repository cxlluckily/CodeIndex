package me.roohom.apps;

import me.roohom.bean.Person;
import me.roohom.format.Bytes2InputFormat;
import me.roohom.format.BytesInputFormat;
import me.roohom.source.BinaryFileSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DeserializeApplication {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<ArrayList<String>> dataStreamSource = env.readFile(new BytesInputFormat(), "data/obj/people.txt");
        //DataStreamSource<ArrayList<String>> dataStreamSource = env.readFile(new BytesInputFormat(), "data/2021-11-17--15/part-5-3");

        dataStreamSource.flatMap(new FlatMapFunction<ArrayList<String>, String>() {
            @Override
            public void flatMap(ArrayList<String> value, Collector<String> out) throws Exception {
                value.forEach(x -> out.collect(x));
            }
        }).setParallelism(1)
        .print();

        //DataStreamSource<Person> streamSource = env.addSource(new BinaryFileSource("data/obj/people.txt"));
        //streamSource.print();
        //
        //env.readTextFile("");


        //DataStreamSource<byte[]> stringDataStreamSource = env.readFile(new Bytes2InputFormat(new Path("data/obj/people.txt")), "data/obj/people.txt");
        //
        env.readTextFile("");
        env.execute();
    }
}
