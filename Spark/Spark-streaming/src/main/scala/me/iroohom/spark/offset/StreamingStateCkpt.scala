package me.iroohom.spark.offset

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies, LocationStrategy}


/**
 * SparkStreaming实现状态累计实时统计：DStream#mapWithState,当流式应用停止以后，再次启动时：
 * - 其一：继续上次消费Kafka数据偏移量消费数据：MetaData
 * - 其二：继续上次应用停止的状态累加更新状态：State
 */
object StreamingStateCkpt {
  // 检查点目录
  val CKPT_DIR: String = s"datas/streaming/state-ckpt-${System.currentTimeMillis()}"


  /**
   * 处理数据
   *
   * @param ssc 流式处理上下文对象
   */
  def processData(ssc: StreamingContext): Unit = {
    val kafkaDStream: DStream[ConsumerRecord[String, String]] = {
      //位置策略
      val locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent
      //读取的Topic
      val topic = Array("search-log-topic")

      //消费kafka配置参数
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "node1.itcast.cn:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "group_id_streaming_0002",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )

      //消费策略
      val consumerKafkaStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe(
        topic, kafkaParams
      )

      KafkaUtils.createDirectStream(ssc, locationStrategy, consumerKafkaStrategy)
    }

    val resultDStream: DStream[(String, Int)] = kafkaDStream.transform(rdd =>
      rdd.filter(record => record != null && record.value().trim.split(",").length == 4)
        .map(record =>
          //获取Message消息
        {
          val message = record.value().split(",").last
          message -> 1
        })
        .reduceByKey(_ + _)
    )


    /**
     * def function[KeyType, ValueType, StateType, MappedType](
     * mappingFunction: (KeyType, Option[ValueType], State[StateType]) => MappedType
     * ): StateSpec[KeyType, ValueType, StateType, MappedType]
     */
    val spec = StateSpec.function(
      (keyword: String, countOption: Option[Int], state: State[Int]) => {
        // a. 获取当前批次中搜索词搜索次数
        val currentState: Int = countOption.getOrElse(0)
        // b. 从以前状态中获取搜索词搜索次数
        val previousState = state.getOption().getOrElse(0)
        // c. 搜索词总的搜索次数
        val latestState = currentState + previousState
        // d. 更行状态
        state.update(latestState)
        // e. 返回最新省份销售订单额
        (keyword, latestState)
      }
    )


    /**
     * def mapWithState[StateType: ClassTag, MappedType: ClassTag](
     * spec: StateSpec[K, V, StateType, MappedType]
     * ): MapWithStateDStream[K, V, StateType, MappedType]
     */
    val stateDStream: MapWithStateDStream[String, Int, Int, (String, Int)] = resultDStream.mapWithState(spec)

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
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))

      new StreamingContext(sparkConf, Seconds(5))
    }
    ssc.checkpoint(CKPT_DIR)

    processData(ssc)


    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
