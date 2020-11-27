package me.iroohom.spark.kafka.source

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 使用Structured Streaming从Kafka实时读取数据，进行词频统计，将结果打印到控制台。
 */
object StructuredKafkaSource {
  def main(args: Array[String]): Unit = {
    // 1. 构建SparkSession实例对象，加载流式数据
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    import spark.implicits._


    //设置从KAFKA读取数据
    val kafkaStreamDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("subscribe", "wordsTopic")
      .load()


    val inputStreamDF: Dataset[String] = kafkaStreamDF
      //Selects a set of SQL expressions. This is a variant of `select` that accepts SQL expressions.
      .selectExpr("CAST(VALUE AS STRING)")
      .as[String]

    val resultStreamDF: DataFrame = inputStreamDF
      .as[String]
      .filter(line => line != null && line.trim.length > 0)
      .flatMap(line => line.trim.split("\\s+"))
      .groupBy($"value").count()


    val query = resultStreamDF.writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-wordcount")
      .format("console")
      .option("numRows", "20")
      .option("truncate", "false")
      // TODO: 设置检查点目录
      .option("checkpointLocation", s"datas/structured/ckpt-kafka-${System.nanoTime()}")
      .start() // 启动流式查询
    query.awaitTermination()
    query.stop()
  }
}
