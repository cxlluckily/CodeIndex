package me.iroohom.start

import org.apache.spark.sql.{Dataset, SparkSession}

object SparkStartPoint {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      //通过装饰模式获取实例对象，此种方式为线程安全的
      .getOrCreate()

    import spark.implicits._

    val inputDS: Dataset[String] = spark.read.textFile("datas\\wordcount.data")
    println(s"Count = ${inputDS.count()}")

    inputDS.show(5)

    spark.stop()
  }
}
