package me.iroohom.spark.sinks

import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 使用Structured Streaming从TCP Socket实时读取数据，进行词频统计，将结果保存至MySQL表中
 */
object StructuredMySQLSink {
  def main(args: Array[String]): Unit = {
    // 1. 构建SparkSession实例对象，加载流式数据
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    import spark.implicits._

    val inputStreamDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node1.roohom.cn")
      .option("port", 9999)
      .load()

    val resultStreamDF: DataFrame = inputStreamDF
      //DataFrame类型不安全需要转换为Dataset
      .as[String]
      .filter(line => line != null && line.trim.length > 0)
      .flatMap(line => line.trim.split("\\s+"))
      .groupBy($"value")
      .count()


    val query = resultStreamDF.writeStream
      //更新模式
      .outputMode(OutputMode.Update())
      //设置查询名称
      .queryName("query-wordcount")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreach(new MySQLForeachWriter())
      // 设置检查点目录
      .option("checkpointLocation", "datas/structured/ckpt-wordcount001")
      .start() // 启动流式查询


    query.awaitTermination()
    query.stop()
  }
}
