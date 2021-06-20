package me.iroohom.spark.kafka

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies, LocationStrategy}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * Streaming通过Kafka New Consumer消费者API获取数据
 */
object StreamSourceKafka {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = {
      val sparkConf = new SparkConf()
        //至少是2,最好是3
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      //时间间隔，用于划分流式数据为很多批次
      new StreamingContext(sparkConf, Seconds(5))
    }

    //Kafka参数
    val kafkaParams: Map[String, Object] = Map(
      "bootstrap.servers" -> "node1.roohom.cn:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group_id_streaming_0001",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //Topic
    val topics = Array("wc-topic")

    //位置策略
    val locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent
    //消费策略
    val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe(
      topics, kafkaParams
    )

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc, locationStrategy, consumerStrategy
    )

    val resultDStream: DStream[(String, Int)] = kafkaDStream.transform(rdd => {
      rdd
        //获取Message消息
        .map(record => record.value())
        .filter(line => line != null && line.trim.length > 0)
        .flatMap(line => line.trim.split("\\s+"))
        .map(word => (word, 1))
        .reduceByKey((a, b) => a + b)
    })

    // 将结果数据输出 -> 将每批次的数据处理以后输出
    resultDStream.foreachRDD { (rdd, time) =>
      val batchTime: String = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss")
        .format(new Date(time.milliseconds))
      println()
      println("+=================================================+")
      println(s"+          Time: ${batchTime}              +")
      println("+=================================================+")
      // TODO: 先判断RDD是否有数据，有数据在输出哦
      if (!rdd.isEmpty()) {
        rdd.coalesce(1).foreachPartition { iter => iter.foreach(item => println(item)) }
      }
    }


    //启动Receiver 接收器 Receiver划分流式数据的时间间隔BlockInterval ，默认值为 200ms，通过属性
    //【spark.streaming.blockInterval】设置
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
