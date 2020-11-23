package me.iroohom.dataframe.todf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQLToDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val usersRDD: RDD[(Int, String, Int)] = spark.sparkContext.parallelize(
      Seq(
        (10001, "zhangsan", 23),
        (10002, "lisi", 22),
        (10003, "wangwu", 23),
        (10004, "zhaoliu", 24)
      )
    )

    /**
     * 将RDD转换为DataFrame
     */
    val usersDF: DataFrame = usersRDD.toDF("id", "name", "age")
    usersDF.printSchema()
    usersDF.show(10, truncate = false)
    println("=========================================================================")

    val usersDF2: DataFrame = Seq(
      (10001, "zhangsan", 23),
      (10002, "lisi", 22),
      (10003, "wangwu", 23),
      (10004, "zhaoliu", 24)
    ).toDF("id", "name", "age")
    usersDF2.printSchema()
    usersDF2.show(10, truncate = false)

    spark.stop()
  }
}
