package me.roohom.apps;

import me.roohom.bean.User;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AvroApplication {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Path path = new Path("/Users/roohom/Code/IDEAJ/CodeIndex/flink-lab/data/avro/avro.txt");
        AvroInputFormat<User> avroInputFormat = new AvroInputFormat<>(path, User.class);

        DataStreamSource<User> inputAvroStream = env.createInput(avroInputFormat);
        inputAvroStream.print();

        env.execute();
    }
}
