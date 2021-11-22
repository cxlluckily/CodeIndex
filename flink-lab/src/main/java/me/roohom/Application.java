package me.roohom;

import com.fasterxml.jackson.databind.ObjectMapper;
import me.roohom.bean.Person;
import me.roohom.source.BinaryFileSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {
    private static final String TOPIC = "";
    private static final String GROUP_ID = "test_group";
    private static final String BOOTSTRAP_SERVERS = "";
    private static final String OUTPUT_PATH = "/Users/roohom/Code/IDEAJ/CodeIndex/flink-lab/data";
    private static final String INPUT_PATH = "/Users/roohom/Code/IDEAJ/CodeIndex/flink-lab/data/2021-11-17--15/part-5-0";
    private static final String PERSON_INPUT_PATH = "/Users/roohom/Code/IDEAJ/CodeIndex/flink-lab/data/obj/person.txt";

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

        //Properties properties = new Properties();
        //properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        //properties.setProperty("group.id", GROUP_ID);
        //FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer(
        //        TOPIC,
        //        new Bytes2BytesSchema(),
        //        properties
        //);
        //kafkaConsumer.setStartFromEarliest();
        //
        //DataStreamSource streamSource = env.addSource(kafkaConsumer);
        //
        //StreamingFileSink<byte[]> fileSink = StreamingFileSink
        //        .forRowFormat(new Path(OUTPUT_PATH), new SimpleBytesEncoder<byte[]>("UTF-8"))
        //        .withRollingPolicy(
        //                DefaultRollingPolicy.builder()
        //                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
        //                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
        //                        .withMaxPartSize(1024 * 1024)
        //                        .build()
        //        ).build();
        //
        //streamSource.name("ReadKafka")
        //        .addSink(fileSink).name("SinkFile");

        //ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(new File(PERSON_INPUT_PATH)));
        DataStreamSource<Person> binaryFileSource = env.addSource(new BinaryFileSource(PERSON_INPUT_PATH));
        binaryFileSource.print();


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
