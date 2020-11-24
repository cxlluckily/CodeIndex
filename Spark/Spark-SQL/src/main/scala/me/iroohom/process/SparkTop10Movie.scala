package me.iroohom.process

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * 获取Top10电影（电影评分平均值最高，并且每个电影被评分的次数大于2000)。
 */
object SparkTop10Movie {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._


    val rawRatingsRDD: RDD[String] = spark.sparkContext.textFile("D:\\itcast\\Spark\\Basic\\spark_day04_20201122\\05_数据\\ml-1m\\ratings.dat", minPartitions = 4)
    //    println(s"Count = ${ratingsRDD.count()}")
    //    println(s"First = ${ratingsRDD.first()}")

    /**
     * 数据转换
     */
    val ratingsDF: DataFrame = rawRatingsRDD.filter(line =>
      line != null && line.trim.split("::").length == 4
    ).mapPartitions { iter =>
      iter.map { line =>
        val Array(userId, itemId, rating, timestamp) = line.trim.split("::")

        (userId.toInt, itemId.toInt, rating.toDouble, timestamp.toLong)
      }
    }
      //调用toDF将RDD转换为DataFrame
      .toDF("user_id", "movie_id", "rating", "timestamp")

    /**
     * root
     * |-- user_id: integer (nullable = false)
     * |-- movie_id: integer (nullable = false)
     * |-- rating: double (nullable = false)
     * |-- timestamp: long (nullable = false)
     */
    /**
     * +-------+--------+------+---------+
     * |user_id|movie_id|rating|timestamp|
     * +-------+--------+------+---------+
     * |1      |1193    |5.0   |978300760|
     * |1      |661     |3.0   |978302109|
     * |1      |914     |3.0   |978301968|
     * |1      |3408    |4.0   |978300275|
     * +-------+--------+------+---------+
     */

    //    ratingsDF.printSchema()
    //    ratingsDF.show(10, truncate = false)

    //TODO:基于SQL方式分析 注册视图 写SQL
    ratingsDF.createOrReplaceTempView("view_tmp_rating")

    val top10movieSQL: DataFrame = spark.sql(
      """
        |SELECT
        |   movie_id AS movieId,
        |   ROUND(AVG(rating),2) AS rating,
        |   COUNT(user_id) as count
        |FROM
        |   view_tmp_rating
        |GROUP BY
        |   movie_id
        |HAVING
        |   count > 2000
        |ORDER BY
        |   rating DESC
        |LIMIT
        |   10
        |""".stripMargin)

    top10movieSQL.printSchema()
    top10movieSQL.show(10, truncate = false)

    println("=========================================================")

    //TODO:使用DSL方式解析数据
    import org.apache.spark.sql.functions._

    val top10movieDSL = ratingsDF
//      .select($"timestamp")
      //按照电影ID进行分组
      .groupBy($"movie_id")
      //按照电影评分进行聚合 计数评分的人
      .agg(
        round(avg($"rating"), 2).as("rating"),
        count($"movie_id").as("count")
      )
      //过滤评分超过两千的
      .where($"count".gt(2000))
      //以评分排序，倒序排序
      .orderBy($"rating".desc)
      //只要求取前十即可
      .limit(10)

    top10movieDSL.printSchema()
    top10movieDSL.show(10, truncate = false)



        //TODO：保存结果数据
        /**
         * 因为ratingDF在下面会使用两次，产生了多次使用，于是将其持久化，后续在使用就无需重新创建使得程序更快
         */
        ratingsDF.persist(StorageLevel.MEMORY_AND_DISK)
        //TODO:数据保存至MySQL
        top10movieDSL
          .coalesce(1)
            .write
            .mode(SaveMode.Overwrite)
            .format("jdbc")
            .option("driver","com.mysql.cj.jdbc.Driver")
            .option("url", "jdbc:mysql://localhost:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
            .option("user","root")
            .option("password","123456")
            .option("dbtable","roohom.top10movies")
            .save()

        //TODO：保存到文件
        top10movieDSL
            .coalesce(1)
            .write
            .mode(SaveMode.Overwrite)
            .csv("datas/spark/top10movie")


    spark.stop()
  }
}
