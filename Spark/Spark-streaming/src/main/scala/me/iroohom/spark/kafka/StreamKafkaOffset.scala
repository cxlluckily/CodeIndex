package me.iroohom.spark.kafka


import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 集成Kafka，实时消费Topic中数据，获取每批次数据对应Topic各个分区数据偏移量
 */
object StreamKafkaOffset {

  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = {
      val sparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
        .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")

      new StreamingContext(sparkConf, Seconds(5))
    }

    val locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent

    val kafkaParams: Map[String, Object] = Map(
      "bootstrap.servers" -> "node1.itcast.cn:9092", //
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "groop_id_1001",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe(
      Array("wc-topic"), kafkaParams
    )

    // 使用Kafka New Consumer API消费数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc, locationStrategy, consumerStrategy
    )

    //定义数据用于存储偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty

    val resultDStream: DStream[(String, Int)] = kafkaDStream.transform { rdd =>
      //此时直接针对获取KafkaDStream进行转换操作，rdd属于KafkaRDD，包含相关偏移量信息
      // TODO: 其二、转换KafkaRDD为HasOffsetRanges类型对象，获取偏移量范围
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val resultRDD: RDD[(String, Int)] = rdd
        .map(record => record.value())
        .filter(line => line != null && line.trim.length > 0)
        .flatMap(line => line.trim.split("\\s+"))
        .map(word => (word, 1))
        .reduceByKey((a, b) => a + b)

      resultRDD
    }

    resultDStream.foreachRDD((rdd, time) => {
      val format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
        .format(time.milliseconds)
      println()
      println("+=================================================+")
      println(s"+          Time: ${format}              +")
      println("+=================================================+")

      //有数据再打印
      if (!rdd.isEmpty()) {
        rdd
          .coalesce(1)
          .foreachPartition(iter =>
            iter.foreach(println)
          )
      }

      offsetRanges.foreach { offsetRange =>
        println(s"topic: ${offsetRange.topic}    partition: ${offsetRange.partition}    " +
          s"offsets: ${offsetRange.fromOffset} to ${offsetRange.untilOffset}")
      }
    })


    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
