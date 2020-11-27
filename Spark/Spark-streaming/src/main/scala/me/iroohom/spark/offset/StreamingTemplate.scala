package me.iroohom.spark.offset

import java.lang

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies, LocationStrategy}


/**
 * SparkStreaming流式应用模板Template，将从数据源读取数据、实时处理及结果输出封装到方法中。
 */
object StreamingTemplate {

  /**
   * 抽象一个函数：专门从数据源读取流式数据，经过状态操作分析数据，最终将数据输出
   *
   * @param ssc 流式上下文StreamingContext实例对象
   */
  def processData(ssc: StreamingContext): Unit = {


    val kafkaDStream: DStream[ConsumerRecord[String, String]] = {
      val locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent
      val topic = Array("search-log-topic")

      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "node1.itcast.cn:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "group_id_streaming_0002",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: lang.Boolean)
      )
      //消费者策略
      val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe(
        topic, kafkaParams
      )

      //采用消费者新API获取数据
      KafkaUtils.createDirectStream(ssc, locationStrategy, consumerStrategy)

    }

    /**
     * 数据聚合及ETL
     */
    val reducedDStream: DStream[(String, Int)] = kafkaDStream.transform(rdd => {
      val reducedRDD: RDD[(String, Int)] = rdd
        .filter(record =>
          record != null && record.value().trim.split(",").length == 4
        )
        .map(record => {
          val keyWord = record.value().trim.split(",").last
          keyWord -> 1
        })
        .reduceByKey(_ + _)

      reducedRDD
    })

    /**
     * 使用mapWithState函数状态更新, 针对每条数据进行更新状态
     */
    val spec: StateSpec[String, Int, Int, (String, Int)] = StateSpec.function(
      (keyWord: String, count: Option[Int], state: State[Int]) => {
        val currentState = count.getOrElse(0)
        val previousState = state.getOption().getOrElse(0)
        val latestState = previousState + currentState

        //更新状态
        state.update(latestState)

        (keyWord, latestState)
      }
    )

    val stateDStream: MapWithStateDStream[String, Int, Int, (String, Int)] = reducedDStream.mapWithState(spec)

    stateDStream.foreachRDD((rdd, time) => {
      val batchTime: String = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(time.milliseconds)
      println()
      println("+=================================================+")
      println(s"+          Time: ${batchTime}              +")
      println("+=================================================+")

      if (!rdd.isEmpty()) {
        rdd.coalesce(1)
          .foreachPartition(iter => iter.foreach(println))
      }
    })
  }


  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = {
      val sparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[3]")
        .set("spark.streaming.kafka.maxRatePerPartition", "10000")


      new StreamingContext(sparkConf, Seconds(5))
    }

    ssc.checkpoint(s"datas/spark/chpt-${System.currentTimeMillis()}")

    processData(ssc)


    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
