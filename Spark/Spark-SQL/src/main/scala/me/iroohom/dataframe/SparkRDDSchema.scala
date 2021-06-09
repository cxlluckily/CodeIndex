package me.iroohom.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

/**
 * 自定义Schema方式转换RDD为DataFrame
 */
object SparkRDDSchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()
    import spark.implicits._


    val rawRatingsRDD: RDD[String] = spark.sparkContext
      .textFile("D:\\itcast\\Spark\\Basic\\spark_day04_20201122\\05_数据\\ml-100k\\u.data", minPartitions = 2)

    /**
     * 筛选数据
     */
    val rowsRDD: RDD[Row] = rawRatingsRDD
      .filter(line => line != null && line.trim.split("\t").length > 0)
      .mapPartitions { iter =>
        iter.map { line =>
          val Array(userId, itemId, rating, timestamp) = line.trim.split("\t")

          Row(userId, itemId, rating.toDouble, timestamp.toLong)
        }
      }
    /**
     * 自定义Schema
     */
    val rowSchema: StructType = StructType(
      Array(
        StructField("userId", StringType, nullable = true),
        StructField("itemId", StringType, nullable = true),
        StructField("rating", DoubleType, nullable = true),
        StructField("timestamp", LongType, nullable = true)
      )
    )
    /**
     * 应用函数createDataFrame
     */
    val ratingDF: DataFrame = spark.createDataFrame(rowsRDD, rowSchema)
    ratingDF.printSchema()
    ratingDF.show(10, truncate = false)

    println("================================================================")
    val ratingDS: Dataset[MovieRating] = ratingDF.as[MovieRating]
    ratingDS.printSchema()
    ratingDS.show(10, truncate = false)

    println("================================================================")
    val ratingDF2: DataFrame = ratingDS.toDF()


    spark.stop()
  }
}
