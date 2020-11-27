package me.iroohom.spark.iot

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 对物联网设备状态信号数据，实时统计分析:
 * 1）、信号强度大于30的设备
 * 2）、各种设备类型的数量
 * 3）、各种设备类型的平均信号强度
 */
object IotStreamingOnline {
  def main(args: Array[String]): Unit = {
    // 1. 构建SparkSession会话实例对象，设置属性信息
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      .config("spark.sql.shuffle.partitions", "3")
      .getOrCreate()
    // 导入隐式转换和函数库
    import spark.implicits._
    import org.apache.spark.sql.functions._


    val iotKafkaStreamDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("subscribe", "iotTopic")
      .option("maxOffsetPerTrigger", "100000")
      .load()

    //数据ETL
    val etlStreamDF: DataFrame = iotKafkaStreamDF
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .filter(message => message != null && message.trim.length > 0)

      /**
       * TODO:最重要的部分在这里 核心
       */
      .select(
        get_json_object($"value", "$.device").as("device"),
        get_json_object($"value", "$.deviceType").as("deviceType"),
        get_json_object($"value", "$.signal").as("signal"),
        get_json_object($"value", "$.time").as("time")
      )


    /**
     * DSL分析数据
     */
    val resultStreamDF: DataFrame = etlStreamDF
      //过滤得到信号值大于30的设备
      .filter($"signal" > 30)
      .groupBy($"deviceType")
      .agg(
        //各种设备类型的数量
        count($"deviceType").as("count_device"),
        //各种设备类型的平均信号强度
        round(avg($"signal"), 2).as("avg_signal")
      )

    val query = resultStreamDF
      .writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .option("numRows", "10")
      .option("truncate", "false")
      .start()
    query.awaitTermination()
    query.stop()

  }
}
