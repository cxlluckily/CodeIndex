package me.iroohom.spark.output

import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 使用Structured Streaming从TCP Socket实时读取数据，进行词频统计，将结果打印到控制台。
 */
object StructuredQueryOutput {
  def main(args: Array[String]): Unit = {
    // 1. 构建SparkSession实例对象，加载流式数据
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    import spark.implicits._

    // 2. 从TCP Socket读取流式数据
    val inputStreamDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node1")
      .option("port", 9999)
      .load()

    inputStreamDF.printSchema()


    //调用DataFrame API运行处理分析数据
    val resultStreamDF: DataFrame = inputStreamDF
      //将DataFrame转换为Dataset 因为前者是类型不安全的而后者是类型安全的
      .as[String]
      .filter(line => null != line && line.trim.length > 0)
      .flatMap(line => line.trim.split("\\s+"))
      //按照单词分组
      .groupBy($"value")
      .count()


    val query = resultStreamDF.writeStream
      //更新模式
      .outputMode(OutputMode.Update())
      //设置插入名称
      .queryName("query-wordcount")
      //设置触发器
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("console")
      .option("numRows", "20")
      .option("truncate", "false")
      // TODO: 设置检查点目录
      .option("checkpointLocation", "datas/structured/ckpt-wordcount")
      .start() // 启动流式查询


    query.awaitTermination()
    query.stop()
  }
}
