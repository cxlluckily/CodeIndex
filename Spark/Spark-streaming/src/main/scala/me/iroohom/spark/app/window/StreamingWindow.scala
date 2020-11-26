package me.iroohom.spark.app.window

import me.iroohom.spark.app.StreamingContextUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
 * 实时消费Kafka Topic数据，每隔一段时间统计最近搜索日志中搜索词次数
 * 批处理时间间隔：BatchInterval = 2s
 * 窗口大小间隔：WindowInterval = 4s
 */
object StreamingWindow {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContextUtils.getStreamingContext(this.getClass, 2)
    ssc.checkpoint(s"datas/spark/ckpt-${this.getClass.getSimpleName.stripSuffix("$")}-${System.currentTimeMillis()}")


    //从Kafka消费数据，采用New Consumer API
    val kafkaDStream: DStream[ConsumerRecord[String, String]] = StreamingContextUtils.consumerKafka(ssc, "search-log-topic")

    //设置窗口，大小为4秒，滑动为2秒 划分两秒一个窗口统计过去四秒的数据 spark中没有滚动窗口一说，只能通过窗口来达到滚动的效果
    // def window(windowDuration: Duration, slideDuration: Duration): DStream[T]
    val windowDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.window(Seconds(4), Seconds(2))

    //对当前窗口数据进行统计聚合
    val resultDStream: DStream[(String, Int)] = windowDStream.transform(rdd =>
      rdd
        .filter(record => record != null && record.value().trim.split(",").length == 4)
        .map { record =>
          //获取每条消息
          val message = record.value()
          //获取搜索关键词
          val searchWord = message.trim.split(",").last
          //返回二元组 封装搜索词设置搜索次数为1
          searchWord -> 1
        }
        .reduceByKey(_ + _)
    )

    resultDStream.foreachRDD((rdd, time) => {
      val batchTime = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(time.milliseconds)
      println()
      println("+=================================================+")
      println(s"+          Time: ${batchTime}              +")
      println("+=================================================+")

      if (!rdd.isEmpty()) {
        rdd.coalesce(1)
          .foreachPartition(iter => iter.foreach(println))
      }
    }
    )


    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }


}
