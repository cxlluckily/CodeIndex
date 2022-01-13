package com.svw.bdp.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.jcodings.util.Hash;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SchemaMapFunction extends RichMapFunction<String, Row> {

    //维护最全的字段及其类型
    private transient MapState<String, String> columnMs = null;

    private final ObjectMapper objectMapper;

    public SchemaMapFunction(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<String, String>(
                "columnMapState",
                String.class,
                String.class
        );
        columnMs = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    @Override
    public Row map(String value) throws Exception {
        HashMap<String, String> oldColumnMap = new HashMap<>();

        for (String key : columnMs.keys()) {
            oldColumnMap.put(key, columnMs.get(key));
        }

        oldColumnMap.forEach(
                (x,y) -> {
                    System.out.println("old -> k :" + x);
                    System.out.println("old -> v :" + y);
                }
        );

        Map<String, String> map = objectMapper.readValue(value, Map.class);
        List<String> columns = map.keySet().stream().collect(Collectors.toList());
        System.out.println("columns -> " + columns);

        HashMap<String, String> columnTypeMap = new HashMap<>();

        if (oldColumnMap.size() < map.size()) {
            System.out.println("oldColumnMap -> " + oldColumnMap.size());
            System.out.println("newColumnMap -> " + map.size());
            for (String x : columns) {
                try {
                    System.out.println("x -> " + x);
                    System.out.println("v -> " + parseKVPairType(map.get(x)));

                    columnTypeMap.put(x, parseKVPairType(map.get(x)));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            System.out.println("Hive JDBC HERE MAYBE...");
        }
        System.out.println("columnTypeMap -> " + columnTypeMap);
        columnMs.putAll(columnTypeMap);

        Row row = new Row(map.size());
        for (int i = 0; i < map.size(); i++) {
            row.setField(i, map.get(columns.get(i)));
        }

        return row;
    }


    /**
     * 解析Json中值的类型
     *
     * @param value json键值对的值
     * @return 对象类型
     */
    public String parseKVPairType(Object value) {
        if (value instanceof Integer) {
            return "INT";
        }
        if (value instanceof Double) {
            return "DOUBLE";
        }
        if (value instanceof Float) {
            return "FLOAT";
        }
        return "STRING";
    }
}
