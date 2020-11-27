package me.iroohom.spark.window

import java.sql.Timestamp

import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


/**
 * 基于Structured Streaming 模块读取TCP Socket读取数据，进行事件时间窗口统计词频WordCount，将结果打印到控制台
 * TODO：每5秒钟统计最近10秒内的数据（词频：WordCount)
 *
 * EventTime即事件真正生成的时间：
 * 例如一个用户在10：06点击 了一个按钮，记录在系统中为10：06
 * 这条数据发送到Kafka，又到了Spark Streaming中处理，已经是10：08，这个处理的时间就是process Time。
 *
 * 测试数据：
 * 2019-10-12 09:00:02,cat dog
 * 2019-10-12 09:00:03,dog dog
 * 2019-10-12 09:00:07,owl cat
 * 2019-10-12 09:00:11,dog
 * 2019-10-12 09:00:13,owl
 **/
object StructuredWindow {
  def main(args: Array[String]): Unit = {
    // 1. 构建SparkSession实例对象，传递sparkConf参数
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 2. 使用SparkSession从TCP Socket读取流式数据
    val inputStreamDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node1")
      .option("port", 9999)
      .load()


    val resultStreamDF: Dataset[Row] = inputStreamDF
      .as[String]
      .filter(line => line != null && line.trim.split(",").length == 2)
      // 将每行数据进行分割单词: 2019-10-12 09:00:02,cat dog
      // 使用flatMap函数以后 -> (2019-10-12 09:00:02, cat)  ,  (2019-10-12 09:00:02, dog)
      .flatMap { line =>
        val arr = line.trim.split(",")

        arr(1).split("\\s+").map(word => (Timestamp.valueOf(arr(0)), word))
      }
      .toDF("insert_timestamp", "word")
      //设置基于事件时间（event time）窗口 -> insert_timestamp, 每5秒统计最近10秒内数据
      //先按照窗口分组、2. 再对窗口中按照单词分组、 3. 最后使用聚合函数聚合
      .groupBy(
        window($"insert_timestamp", "10 seconds", "5 seconds"),
        $"word"
      )
      .count()
      .orderBy($"window")

    resultStreamDF.printSchema()

    val query = resultStreamDF.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .format("console")
      .option("numRows", "100")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination()
    query.stop()
  }
}


