package me.iroohom.spark.app.state

import me.iroohom.spark.app.StreamingContextUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

object StreamingUpdateState {

  def main(args: Array[String]): Unit = {


    val ssc: StreamingContext = StreamingContextUtils.getStreamingContext(this.getClass, 10)

    ssc.checkpoint(s"datas/spark/chpt-${System.currentTimeMillis()}")

    val kafkaDStream: DStream[ConsumerRecord[String, String]] = StreamingContextUtils.consumerKafka(ssc, "search-log-topic")

    //对当前批次进行聚合统计
    val batchResultDStream: DStream[(String, Int)] =
      kafkaDStream.transform { rdd =>
        rdd
          .filter((record: ConsumerRecord[String, String]) => record != null && record.value().trim.split(",").length == 4)
          .map { record =>
            //获取kafka数据的消息Message
            val message = record.value()
            //获取搜索的关键词
            val searchWord = message.split(",").last
            //返回二元组
            searchWord -> 1
          }
          //聚合
          .reduceByKey((a, b) => a + b)
      }

    /**
     * 将当前批出聚合结果与以前状态数据进行聚合操作（状态更新）
     * def updateStateByKey[S: ClassTag](
     * updateFunc: (Seq[V], Option[S]) => Option[S]
     * ): DStream[(K, S)]
     */
    val stateStream: DStream[(String, Int)] = batchResultDStream.updateStateByKey(
      (values: Seq[Int], state: Option[Int]) => {
        //获取当前批次中状态
        val currentState = values.sum
        //获取以前转换
        val previousState = state.getOrElse(0)
        //合并
        val latestState = currentState + previousState

        Some(latestState)
      }
    )


    /**
     * 输出每批次聚合结果
     */
    stateStream.foreachRDD { (rdd, time) =>
      val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
        .format(time.milliseconds)
      println()
      println("+=================================================+")
      println(s"+          Time: ${dateFormat}              +")
      println("+=================================================+")

      if (!rdd.isEmpty()) {
        rdd.coalesce(1)
          .foreachPartition(iter => iter.foreach(println))
      }
    }


    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}