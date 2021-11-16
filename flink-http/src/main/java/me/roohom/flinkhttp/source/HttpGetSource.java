package me.roohom.flinkhttp.source;

import me.roohom.flinkhttp.utils.HttpUtil;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class HttpGetSource extends RichSourceFunction<String> {

    private volatile boolean isRunning = true;
    private String url;
    private long requestInterval;
    private DeserializationSchema<String> deserializer;
    // count out event
    private transient Counter counter;

    public HttpGetSource(String url, long requestInterval, DeserializationSchema<String> deserializer) {
        this.url = url;
        this.requestInterval = requestInterval;
        this.deserializer = deserializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        counter = new SimpleCounter();
        this.counter = getRuntimeContext()
                .getMetricGroup()
                .counter("myCounter");
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            try {
                // receive http message, csv format
                String message = HttpUtil.doGet(url);
                // deserializer csv message
                ctx.collect(deserializer.deserialize(message.getBytes()).toString());
                this.counter.inc();

                Thread.sleep(requestInterval);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

}
