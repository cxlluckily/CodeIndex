package me.iroohom.spark.kafka.sink

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 实时从Kafka Topic消费基站日志数据，过滤获取通话转态为success数据，再存储至Kafka Topic中
 * 1、从KafkaTopic中获取基站日志数据（模拟数据，JSON格式数据）
 * 2、ETL：只获取通话状态为success日志数据
 * 3、最终将ETL的数据存储到Kafka Topic中
 */
object StructuredEtlSink {
  def main(args: Array[String]): Unit = {
    // 1. 构建SparkSession实例对象，加载流式数据
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    import spark.implicits._


    val kafkaStreamDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("subscribe", "stationTopic")
      .load()


    //从topic中读取数据并对数据进行ETL操作
    val etlStreamDF: Dataset[String] = kafkaStreamDF
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      //过滤数据
      .filter(message =>
        message != null && message.trim.split(",").length == 6 && "success".equals(message.trim.split(",")(3))
      )

    val query = etlStreamDF
      .writeStream
      .outputMode(OutputMode.Append())
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("topic", "etlTopic")
      // 设置检查点目录
      .option("checkpointLocation", s"datas/structured/kafka-etl-1001")
      // 流式应用，需要启动start
      .start()
    query.awaitTermination()
    query.stop()

  }
}
