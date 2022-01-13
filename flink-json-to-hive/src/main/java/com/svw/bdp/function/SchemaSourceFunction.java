package com.svw.bdp.function;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Map;

public class SchemaSourceFunction extends RichSourceFunction<Map<String, String>> {
    @Override
    public void run(SourceContext<Map<String, String>> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
