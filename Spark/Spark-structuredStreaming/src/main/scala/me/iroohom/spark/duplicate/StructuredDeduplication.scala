package me.iroohom.spark.duplicate

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * StructuredStreaming对流数据按照某些字段进行去重操作，比如实现UV类似统计分析
 * TODO:待测试
 */
object StructuredDeduplication {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._


    val inputTable: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node1")
      .option("port", 9999)
      .load()

    val resultTableDF: DataFrame = inputTable
      .as[String]
      .filter(line => line != null && line.trim.length > 0)
      .select(
        get_json_object($"value", "$.eventTime").as("eventTime"),
        get_json_object($"value", "$.eventType").as("eventType"),
        get_json_object($"value", "$.userID").as("userID")
      )
      //VITAL: 按照userID和eventType进行去重
      .dropDuplicates("userID", "eventType")
      .groupBy($"userID", $"eventType")
      .count()


    val query = resultTableDF
      .writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .option("numRows", "100")
      .option("truncate", "false")
      //VITAL:流式应用需要启动
      .start()

    query.awaitTermination()
    query.stop()

  }
}
