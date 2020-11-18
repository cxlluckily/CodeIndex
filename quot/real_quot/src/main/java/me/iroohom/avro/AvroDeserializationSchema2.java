package me.iroohom.avro;

import me.iroohom.config.QuotConfig;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * @ClassName: AvroDeserializationSchema2
 * @Author: Roohom
 * @Function: Avro反序列化
 * @Date: 2020/10/31 22:01
 * @Software: IntelliJ IDEA
 */
public class AvroDeserializationSchema2<T> implements DeserializationSchema<T> {

    private String topicName;

    //构建构造方法
    public AvroDeserializationSchema2(String topicName) {
        this.topicName = topicName;
    }


    @Override
    public T deserialize(byte[] message) throws IOException {

        SpecificDatumReader<T> datumReader = null;
        if (topicName.equals(QuotConfig.config.getProperty("sse.topic"))) {
            datumReader = new SpecificDatumReader(SseAvro.class);
        } else {
            datumReader = new SpecificDatumReader(SzseAvro.class);
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(message);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bis, null);

        T read = datumReader.read(null, decoder);
        return read;
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    /**
     * 返回数据类型
     *
     * @return
     */
    @Override
    public TypeInformation<T> getProducedType() {

        TypeInformation<T> of = null;
        if (topicName.equals(QuotConfig.config.getProperty("sse.topic"))) {
            of = (TypeInformation<T>) TypeInformation.of(SseAvro.class);
        } else {
            of = (TypeInformation<T>) TypeInformation.of(SzseAvro.class);
        }

        return of;
    }

}
