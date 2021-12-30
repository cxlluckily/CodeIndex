package me.roohom.broadcast;

import lombok.SneakyThrows;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.HashMap;

public class BroadcastApplication {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Integer> integers = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            integers.add(i);
        }
        MapStateDescriptor<Integer, String> mapStateDescriptor = new MapStateDescriptor<Integer, String>(
                "IntegerStringMapState",
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO
        );

        HashMap<Integer, String> intStrMap = new HashMap<>();
        intStrMap.put(1, "21-29");
        intStrMap.put(2, "33-37");
        intStrMap.put(3, "79-89");


        DataStreamSource<Integer> integerDataStream = env.fromCollection(integers);


        env.execute();
    }
}
