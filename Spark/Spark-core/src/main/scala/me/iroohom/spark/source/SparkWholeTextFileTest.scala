package me.iroohom.spark.source

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWholeTextFileTest {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val sparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")

      new SparkContext(sparkConf)
    }


    val inputRDD = sc.wholeTextFiles("D:\\roohom\\Spark\\Basic\\spark_day02_20201119\\05_数据\\ratings100", minPartitions = 2)
      .flatMap(tuple => tuple._2.split("\\s+"))

    println(s"Partitions Number is ${inputRDD.getNumPartitions}")
    println(s"Count = ${inputRDD.count()}")

    sc.stop()
  }
}
