package me.iroohom.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 采用反射的方式将RDD转换为DataFrame和Dataset
 */
object SparkRDDInferring {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()
    import spark.implicits._


    /**
     * // 读取电影评分数据u.data, 每行数据有四个字段，使用制表符分割
     * // user id | item id | rating | timestamp
     */

    val rawRatingsRDD: RDD[String] = spark.sparkContext
      .textFile("D:\\roohom\\Spark\\Basic\\spark_day04_20201122\\05_数据\\ml-100k\\u.data", minPartitions = 2)

    val ratingsRDD: RDD[MovieRating] = rawRatingsRDD
      //过滤数据
      //      .filter(line => line != null && line.trim.split("\t").length == 4)
      //对每个分区操作
      .mapPartitions { iter =>
        //分区内的数据放在迭代器
        iter.map { line =>
          //封装数据到样例类中
          val Array(userId, itemId, rating, timestamp) = line.trim.split("\t")

          MovieRating(userId, itemId, rating.toDouble, timestamp.toLong)
        }
      }
    /**
     * 将RDD转换为DataFrame Dataset
     */
    val ratingsDF: DataFrame = ratingsRDD.toDF()
    ratingsDF.printSchema()
    ratingsDF.show(10, truncate = false)

    println("=================================================================")


    val ratingsDS: Dataset[MovieRating] = ratingsRDD.toDS()
    ratingsDS.printSchema()
    ratingsDS.show(10, truncate = false)


    spark.stop()
  }
}
