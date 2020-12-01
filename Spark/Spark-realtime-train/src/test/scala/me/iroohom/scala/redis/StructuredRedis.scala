package me.iroohom.scala.redis

import me.iroohom.config.ApplicationConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 使用Structured Streaming从TCP Socket实时读取数据，进行词频统计，将结果存储到Redis数据库
 */
object StructuredRedis {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    import spark.implicits._

    //从TCP读取数据
    val inputStreamDF: DataFrame = spark
      .readStream
      .format("socket")
      .option("host", "node1")
      .option("port", "9999")
      .load()

    //Wordcount
    val resultStreamDF: DataFrame = inputStreamDF
      //DF转换为DS
      .as[String]
      .filter(line => line != null && line.trim.length > 0)
      .flatMap(line => line.trim.split("\\s+"))
      .groupBy($"value")
      .count()


    //VITAL: 行转列，再写到Redis ，再启动流式程序
    val query = resultStreamDF
      .writeStream
      .outputMode(OutputMode.Update())
      //VITAL: 注意使用foreachBatch 对每一批次操作
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        //VITAL: 行转列
        batchDF
          .groupBy()
          .pivot($"value").sum("count")
          //字面值
          .withColumn("type", lit("spark"))
          .write
          .mode(SaveMode.Append)
          //设置输出格式是Redis数据
          .format("org.apache.spark.sql.redis")
          .option("host", ApplicationConfig.REDIS_HOST)
          .option("port", ApplicationConfig.REDIS_PORT)
          .option("dbNum", ApplicationConfig.REDIS_DB)
          .option("table", "wordcount")
          .option("key.column", "type")
          .save()
      }
      //流式程序需要启动
      .start()

    query.awaitTermination()
    query.stop()

  }
}
