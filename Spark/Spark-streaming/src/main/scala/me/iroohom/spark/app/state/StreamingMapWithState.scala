package me.iroohom.spark.app.state

import me.iroohom.spark.app.StreamingContextUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.{State, StateSpec, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream}

/**
 * 实时消费Kafka Topic数据，累加统计各个搜索词的搜索次数，实现百度搜索风云榜
 */

object StreamingMapWithState {
  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext = StreamingContextUtils.getStreamingContext(this.getClass, 10)
    //设置检查点存储路径
    ssc.checkpoint(s"datas/spark/chpt-${System.currentTimeMillis()}")

    //从Kafka消费数据，采用New Consumer API
    val kafkaDStream: DStream[ConsumerRecord[String, String]] = StreamingContextUtils.consumerKafka(ssc, "search-log-topic")

    //对当前批次进行聚合统计

    val batchResultStream: DStream[(String, Int)] = kafkaDStream.transform { rdd =>
      rdd
        .filter(record => record != null && record.value().trim.split(",").length == 4)
        .map { record =>
          //获取每条kafka数据的消息Message
          val message = record.value()
          //获取搜索关键词
          val searchWord = message.split(",").last

          //返回元组 每个词设置搜索次数为1
          searchWord -> 1
        }
        //按照搜索词统计分组统计搜索次数 TODO：此处属于性能优化
        .reduceByKey(_ + _)
    }

    /**
     * def function[KeyType, ValueType, StateType, MappedType](
     * mappingFunction: (KeyType, Option[ValueType], State[StateType]) => MappedType
     * )
     */
    val spec: StateSpec[String, Int, Int, (String, Int)] = StateSpec.function(
      (key: String, count: Option[Int], state: State[Int]) => {
        //获取当前key状态
        val currentState = count.getOrElse(0)
        //获取之前的key的状态
        val previousState = state.getOption().getOrElse(0)
        //状态累加得到最新的状态
        val latestState = previousState + currentState

        //返回二元组 当前key与当前状态值
        key -> latestState
      }
    )

    /**
     * def mapWithState[StateType: ClassTag, MappedType: ClassTag](
     * spec: StateSpec[K, V, StateType, MappedType]
     * )
     */
    val stateDStream: DStream[ (String, Int)] = batchResultStream.mapWithState[Int, (String, Int)](spec)

    stateDStream.foreachRDD((rdd, time) => {
      val batchTime = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(time.milliseconds)
      println()
      println("+=================================================+")
      println(s"+          Time: ${batchTime}              +")
      println("+=================================================+")


      if (!rdd.isEmpty()) {
        rdd.coalesce(1)
          .foreachPartition(iter => iter.foreach(println))
      }
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
