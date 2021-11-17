package me.roohom;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tendcloud.enterprise.analytics.collector.message.TDMessage;
import me.roohom.deserializer.Bytes2BytesSchema;
import me.roohom.encoder.SimpleBytesEncoder;
import me.roohom.format.BytesInputFormat;
import me.roohom.source.BinarySource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.BinaryInputFormat;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Application {
    private static final String TOPIC = "";
    private static final String GROUP_ID = "test_group";
    private static final String BOOTSTRAP_SERVERS = "";
    private static final String OUTPUT_PATH = "/Users/roohom/Code/IDEAJ/CodeIndex/flink-lab/data";
    private static final String INPUT_PATH = "/Users/roohom/Code/IDEAJ/CodeIndex/flink-lab/data/2021-11-17--15/part-5-0";

    public static void main(String[] args) throws Exception {
        Logger LOG = LoggerFactory.getLogger(Application.class);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //CHECKPOINT
        env.enableCheckpointing(6000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(6000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);

        ObjectMapper objectMapper = new ObjectMapper();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.setProperty("group.id", GROUP_ID);
        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer(
                TOPIC,
                new Bytes2BytesSchema(),
                properties
        );
        kafkaConsumer.setStartFromEarliest();

        DataStreamSource streamSource = env.addSource(kafkaConsumer);

        StreamingFileSink<byte[]> fileSink = StreamingFileSink
                .forRowFormat(new Path(OUTPUT_PATH), new SimpleBytesEncoder<byte[]>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                .withMaxPartSize(1024 * 1024)
                                .build()
                ).build();

        streamSource.name("ReadKafka")
                .addSink(fileSink).name("SinkFile");


        //DataStreamSource<byte[]> binaryFileSource = env.addSource(new BinarySource(INPUT_PATH));
        //
        //binaryFileSource.map(new MapFunction<byte[], Object>() {
        //    @Override
        //    public Object map(byte[] value) throws Exception {
        //        try {
        //            TDMessage tdMessage = (TDMessage) new ObjectInputStream(new ByteArrayInputStream(value)).readObject();
        //            Object eventPackage = new ObjectInputStream(new ByteArrayInputStream((byte[]) tdMessage.getData())).readObject();
        //            return objectMapper.writeValueAsString(eventPackage);
        //        } catch (Exception e) {
        //            LOG.info("This message cannot cast to a EventPackage -> {}", new String(value, "UTF-8"));
        //            e.printStackTrace();
        //        }
        //        return "";
        //    }
        //})
        //.print();

        env.execute();

    }
}
