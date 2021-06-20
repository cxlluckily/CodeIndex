package me.iroohom.spark.offsetmysql

import java.util.Date
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
 * SparkStreaming从Kafka Topic实时消费数据，手动管理消费偏移量，保存至MySQL数据库表
 */
object StreamingManagerOffsets {
  /**
   * 抽象一个函数：专门从数据源读取流式数据，经过状态操作分析数据，最终将数据输出
   */
  def processData(ssc: StreamingContext): Unit = {
    // 1. 从Kafka Topic实时消费数据
    val groupId: String = "group_id_10001" // 消费者GroupID
    val kafkaDStream: DStream[ConsumerRecord[String, String]] = {
      // i.位置策略
      val locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent
      // ii.读取哪些Topic数据
      val topics = Array("search-log-topic")
      // iii.消费Kafka 数据配置参数
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "node1.roohom.cn:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> groupId,
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )
      // iv.消费数据策略
      // TODO: 从MySQL数据库表中获取偏移量信息
      val offsetsMap: Map[TopicPartition, Long] = OffsetsUtils.getOffsetsToMap(topics, groupId)
      val consumerStrategy: ConsumerStrategy[String, String] = if (offsetsMap.isEmpty) {
        // TODO: 如果第一次消费topic数据，此时MySQL数据库表中没有偏移量信息, 从最大偏移量消费数据
        ConsumerStrategies.Subscribe(topics, kafkaParams)
      } else {
        // TODO: 如果不为空，指定消费偏移量
        ConsumerStrategies.Subscribe(topics, kafkaParams, offsetsMap)
      }
      // v.采用消费者新API获取数据
      KafkaUtils.createDirectStream(ssc, locationStrategy, consumerStrategy)
    }
    // TODO: 其一、创建空Array数组，指定类型
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    // 2. 词频统计，实时累加统计
    // 2.1 对数据进行ETL和聚合操作
    val reduceDStream: DStream[(String, Int)] = kafkaDStream.transform { rdd =>
      // TODO:其二、从KafkaRDD中获取每个分区数据对应的偏移量信息
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val reduceRDD: RDD[(String, Int)] = rdd
        // 过滤不合格的数据
        .filter { record =>
          val message: String = record.value()
          null != message && message.trim.split(",").length == 4
        }
        .map { record =>
          val keyword: String = record.value().trim.split(",").last
          keyword -> 1
        }
        // 按照单词分组，聚合统计
        .reduceByKey((tmp, item) => tmp + item) // TODO: 先聚合，再更新，优化
      // 返回
      reduceRDD
    }

    // 2.2 使用mapWithState函数状态更新, 针对每条数据进行更新状态
    val spec: StateSpec[String, Int, Int, (String, Int)] = StateSpec.function(
      // (KeyType, Option[ValueType], State[StateType]) => MappedType
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
    // 设置状态的初始值，比如状态数据保存Redis中，此时可以从Redis中读取
    //spec.initialState(ssc.sparkContext.parallelize(List("罗志祥" -> 123, "裸海蝶" -> 342)))
    // 调用mapWithState函数进行实时累加状态统计
    val stateDStream: DStream[(String, Int)] = reduceDStream.mapWithState(spec)
    // 3. 统计结果打印至控制台
    stateDStream.foreachRDD { (rdd, time) =>
      val batchTime: String = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss")
        .format(new Date(time.milliseconds))
      println("-------------------------------------------")
      println(s"BatchTime: $batchTime")
      println("-------------------------------------------")
      if (!rdd.isEmpty()) rdd.coalesce(1).foreachPartition {
        _.foreach(println)
      }
      // TODO: 其三、保存每批次数据偏移量到MySQL数据库表中
      OffsetsUtils.saveOffsetsToTable(offsetRanges, groupId)
    }
  }

  // 应用程序入口
  def main(args: Array[String]): Unit = {
    // 1). 构建流式上下文StreamingContext实例对象
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate(
      () => {
        // a. 创建SparkConf对象
        val sparkConf = new SparkConf()
          .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
          .setMaster("local[3]")
        // b. 创建StreamingContext对象
        new StreamingContext(sparkConf, Seconds(5))
      }
    )
    ssc.checkpoint(s"datas/streaming/offsets-${System.nanoTime()}")
    // 2). 实时消费Kafka数据，统计分析
    processData(ssc)
    // 3). 启动流式应用，等待终止（人为或程序异常）
    ssc.start()
    ssc.awaitTermination() // 流式应用启动以后，一直等待终止，否则一直运行
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}