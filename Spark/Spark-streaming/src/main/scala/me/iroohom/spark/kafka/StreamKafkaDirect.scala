package me.iroohom.spark.kafka

import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 集成Kafka，采用Direct式读取数据，对每批次（时间为1秒）数据进行词频统计，将统计结果输出到控制台。
 */
object StreamKafkaDirect {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = {
      val sparkConf = new SparkConf()
        //至少是2,最好是3
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        // 设置数据输出文件系统的算法版本为2
        .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")


      new StreamingContext(sparkConf, Seconds(5))
    }

    // 表示从Kafka Topic读取数据时相关参数设置
    val kafkaParams: Map[String, String] = Map(
      "bootstrap.servers" -> "node1.roohom.cn:9092",
      // 表示从Topic的各个分区的哪个偏移量开始消费数据，设置为最大的偏移量开始消费数据
      "auto.offset.reset" -> "largest"
    )

    val topic: Set[String] = Set("wc-topic")

    // 采用Direct方式从Kafka的Topic中读取数据
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topic
    )

    //仅仅获取kafka Topic中的Value数据也就是Message消息
    val inputDStream: DStream[String] = kafkaDStream.map(tuple => tuple._2)


    //能操作RDD就不要操作DStream 能操作RDD就操作RDD
    val resultDStream: DStream[(String, Int)] = inputDStream.transform(rdd => {
      rdd
        .filter(line => line != null && line.trim.length > 0)
        .flatMap(line => line.split("\\s+"))
        .map(word => (word, 1))
        .reduceByKey((temp, item) => item + temp)

    })

    //定义数据终端，将每批次的结果数据输出
    //    resultDStream.print()
    resultDStream.foreachRDD((rdd, time) => {
      //时间转换 使用lang3包下FastDateFormat日期格式类，属于线程安全的
      //val xx: Time = time
      var format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
          .format(new Date(time.milliseconds))
      println()
      println("+=================================================+")
      println(s"+          Time: ${format}              +")
      println("+=================================================+")


      //如果RDD没有数据就不打印
      if (!rdd.isEmpty()) {
        rdd.coalesce(1)
          .foreachPartition { iter =>
            iter.foreach { item => println(item) }
          }

        rdd.coalesce(1).saveAsTextFile(s"datas/spark/spark-wc-${System.currentTimeMillis()}")
      }

    })


    //启动Receiver 接收器 Receiver划分流式数据的时间间隔BlockInterval ，默认值为 200ms，通过属性
    //【spark.streaming.blockInterval】设置
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
