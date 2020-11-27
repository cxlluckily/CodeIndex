package me.iroohom.spark.source

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/**
 * 使用Structured Streaming从目录中读取文件数据：统计年龄小于25岁的人群的爱好排行榜
 */
object StructuredFileSource {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._


    //从文件系统监控目录，读取CSV文件
    //Schema约束
    val csvSchema: StructType = new StructType()
      .add("name", StringType, nullable = true)
      .add("age", IntegerType, nullable = true)
      .add("hobby", StringType, nullable = true)

    val inputStreamDF: DataFrame = spark.readStream
      .option("sep", ";")
      .option("header", "false")
      //设置Schema约束
      .schema(csvSchema)
      //设置监听目录
      .csv("file:///E:\\datas\\")

    val resultStreamDF: Dataset[Row] = inputStreamDF
      .filter($"age" < 25)
      .groupBy($"hobby").count()
      //这里为什么是count字段？ 因为上一步的count()返回的就是标识为count的字段
      .orderBy($"count".desc)


    val query: StreamingQuery = resultStreamDF.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .option("numRows", "10")
      .option("truncate", "false")
      //启动流式应用
      .start()


    query.awaitTermination()
    query.stop()
  }
}
