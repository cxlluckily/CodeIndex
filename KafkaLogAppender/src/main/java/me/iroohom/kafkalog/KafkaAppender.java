package me.iroohom.kafkalog;

/**
 * @ClassName: KafkaAppender
 * @Author: Roohom
 * @Function: 自定义KafkaLogAppender
 * @Date: 2021/3/24 10:58
 * @Software: IntelliJ IDEA
 */

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import lombok.Data;
import me.iroohom.constant.SystemConstant;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;

import static me.iroohom.transform.AppIdTransform.getAppId;


@Data
public class KafkaAppender extends AppenderBase<ILoggingEvent> {

    private static Logger logger = LoggerFactory.getLogger(KafkaAppender.class);
//    Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    private String bootstrapServers;
    private String logTopic;
    private String containerId;
    FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS",
            TimeZone.getTimeZone("Asia/Shanghai"),
            Locale.CHINA);

    //kafka生产者
    private Producer<String, String> producer;

    @Override
    public void start() {
        super.start();
        if (producer == null) {
            //TODO: 所有生产者的信息通过logback.xml来配置
            Properties props = new Properties();
            props.put("bootstrap.servers", bootstrapServers);
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            //延迟1秒缓存发送，根据日志要求的及时性可调
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<String, String>(props);
        }
    }

    @Override
    protected void append(ILoggingEvent eventObject) {
        //TODO: 获取YARN application ID名称加入日志消息中

        //线程名称
        String threadName = eventObject.getThreadName();
        //日志级别
        Level level = eventObject.getLevel();
        //时间时间戳
        long timeStamp = eventObject.getTimeStamp();
        String now = fastDateFormat.format(timeStamp);
        //logger的名称
        String loggerName = eventObject.getLoggerName();
        //格式化之后的消息，包括数据内容
        String message = eventObject.getFormattedMessage();

        String logMessage = now + SystemConstant.SEP_BLANK + getAppId(containerId) + SystemConstant.SEP_BLANK +
                SystemConstant.OPEN_BRACKET + threadName + SystemConstant.CLOSE_BRACKET + SystemConstant.SEP_BLANK +
                level + SystemConstant.SEP_BLANK + loggerName + SystemConstant.SEP_BLANK + SystemConstant.SEP_MINUS +
                SystemConstant.SEP_BLANK + message;


        String msg = eventObject.getFormattedMessage();
        logger.debug("SENDING LOGS TO KAFKA:" + msg);
        //KEY和VALUE都设置为了消息，有待商榷
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                logTopic, getAppId(containerId), logMessage);
        producer.send(record);
    }
}
