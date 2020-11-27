package me.iroohom.spark.sinks.batch

import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/**
 * 使用Structured Streaming从TCP Socket实时读取数据，进行词频统计，将结果保存至MySQL表中
 */
object StructuredForeachBatch {
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

    val resultStreamDF: DataFrame = inputStreamDF
      .as[String]
      .filter(line => null != line && line.trim.length > 0)
      .flatMap(line => line.trim.split("\\s+"))
      .groupBy($"value")
      .count()


    val query = resultStreamDF
      .writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-wordcount")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      //使用foreach 函数将数据保存到MySQL
      //def foreachBatch(function: VoidFunction2[Dataset[T], java.lang.Long]): DataStreamWriter[T]
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"BatchID: ${batchId}")
        if (!batchDF.isEmpty) {
          batchDF
            //降低分区数目
            .coalesce(1)
            .write
            .mode(SaveMode.Overwrite)
            .format("jdbc")
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("url", "jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
            .option("user", "root")
            .option("password", "123456")
            .option("dbtable", "db_spark.tb_word_count2")
            .save()
        }
      }


      // 设置检查点目录
      .option("checkpointLocation", "datas/structured/ckpt-wordcount002")
      .start() // 启动流式查询
    query.awaitTermination()
    query.stop()
  }
}
