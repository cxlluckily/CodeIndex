package me.iroohom.spark.start

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 使用Structured Streaming从TCP Socket实时读取数据，进行词频统计，将结果打印到控制台。
 */
object StructuredWordCount {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      //spark结构化流的底层就是spark sql 所以需要设置此属性
      .config("spark.sql.shuffle.partitions","2")
      .getOrCreate()

    import spark.implicits._

    //从TCP Socket读取流式数据
    val inputStreamDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node1")
      .option("port", 9999)
      .load()

    inputStreamDF.printSchema()


    val resultStreamDF: DataFrame = inputStreamDF
      .as[String]
      //过滤的到有效数据
      .filter(line => line != null && line.trim.length > 0)
      //合并
      .flatMap(line => line.trim.split("\\s+"))
      .groupBy($"value")
      .count()

    val query = resultStreamDF.writeStream
      //Append为追加模式，将数据直接输出，没有做任何聚合
      //Complete为完全模式，将ResultTable中所有结果数据进行输出
      //Update更新模式，当ResultTable中更新的数据进行输出，类似mapWithState
      .outputMode(OutputMode.Update())
      //设置输出到控制台
      .format("console")
      .option("numRows", "20")
      .option("truncate", "false")
      //启动流式查询
      .start()

    query.awaitTermination()
    query.stop()
  }
}
