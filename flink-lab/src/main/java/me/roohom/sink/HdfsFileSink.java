package me.roohom.sink;

import lombok.Value;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class HdfsFileSink<IN> {

    private String outputPath;

    public SinkFunction<IN> sinkConnect() {
        final StreamingFileSink<IN> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<IN>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build()
                ).build();

        return sink;
    }
}
