package me.iroohom.spark.continuous

import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 从Spark 2.3版本开始，StructuredStreaming结构化流中添加新流式数据处理方式：Continuous processing
 * 持续流数据处理：当数据一产生就立即处理，类似Storm、Flink框架，延迟性达到100ms以下，目前属于实验开发阶段
 * TODO:待测试
 */
object StructuredContinuous {
  def main(args: Array[String]): Unit = {
    // 构建SparkSession实例对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      // 设置Shuffle分区数目
      .config("spark.sql.shuffle.partitions", "3")
      .getOrCreate()
    // 导入隐式转换和函数库
    import spark.implicits._

    // TODO: 1. 从KafkaTopic中获取基站日志数据（模拟数据，文本数据）
    val kafkaStreamDF: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1.itcast.cn:9092")
      .option("subscribe", "stationTopic")
      .load()

    //2
    val etlStreamDF: Dataset[String] = kafkaStreamDF
      // 提取value值，并转换为String类型，最后将DataFrame转换为Dataset
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .filter(message => {
        message != null && message.trim.split(",").length == 6 && "success".equals(message.trim.split(",")(3))
      })

    // 3. 最终将ETL的数据存储到Kafka Topic中
    import scala.concurrent.duration._

    val query = etlStreamDF
      .writeStream
      .queryName("query-state-etl")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.Continuous(1.seconds))
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("topic", "etlTopic")
      .option("checkpointLocation", "data/structured/station-etl-1002")
      //VITAL: 流式应用需要启动
      .start()

    query.awaitTermination()
    query.stop()
  }
}
