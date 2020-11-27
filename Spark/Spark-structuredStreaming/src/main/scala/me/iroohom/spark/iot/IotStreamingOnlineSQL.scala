package me.iroohom.spark.iot

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 对物联网设备状态信号数据，实时统计分析，基于SQL编程
 * 1）、信号强度大于30的设备
 * 2）、各种设备类型的数量
 * 3）、各种设备类型的平均信号强度
 */
object IotStreamingOnlineSQL {
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

    val iotStreamDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("subscribe", "iotTopic")
      // 设置每批次消费数据最大值
      .option("maxOffsetsPerTrigger", "100000") // TODO: 实际项目中，基准压力测试获取
      .load()

    // TODO: 对数据进行ETL操作，提取JSON字符串中各个字段的值
    val etlStreamDF: DataFrame = iotStreamDF
      // 获取value字段的值，转换为String类型
      .selectExpr("CAST(value AS STRING)")
      // 将数据转换Dataset
      .as[String] // 内部字段名为value
      // 过滤脏数据
      .filter(msg => null != msg && msg.trim.length > 0) // 此处过滤，应该解析成功JSON字符串为true，其他为false
      // 数据样本：{"device":"device_65","deviceType":"db","signal":15.0,"time":1600740626186}
      // 提取JSON字符串中各个字段数据
      .select(
        get_json_object($"value", "$.device").as("device"), //
        get_json_object($"value", "$.deviceType").as("deviceType"), //
        get_json_object($"value", "$.signal").as("signal"), //
        get_json_object($"value", "$.time").as("time") //
      )


    etlStreamDF.createOrReplaceTempView("view_tmp_stream_iots")

    /**
     * 1）、信号强度大于30的设备
     * 2）、各种设备类型的数量
     * 3）、各种设备类型的平均信号强度
     */
    val resultStreamDF: DataFrame = spark.sql(
      """
        |SELECT
        |   COUNT(deviceType) AS count_device,
        |   ROUND(AVG(signal),2) AS avg_device_signal
        |FROM
        |   view_tmp_stream_iots
        |WHERE
        |   signal > 30
        |GROUP BY
        |   deviceType
        |""".stripMargin)


    val query = resultStreamDF.writeStream
      .outputMode(OutputMode.Complete())
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println("-------------------------------------------")
        println(s"Batch: $batchId")
        println("-------------------------------------------")
        if (!batchDF.isEmpty) {
          batchDF.coalesce(1).show(50, truncate = false)
        }
      }
      .start()

    query.awaitTermination()
    query.stop()
  }
}
