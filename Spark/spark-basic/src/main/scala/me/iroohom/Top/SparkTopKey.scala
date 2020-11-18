package me.iroohom.Top

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark实现获取词频最高的三个单词
 */
object SparkTopKey {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)

    val sparkContext = new SparkContext(sparkConf)

    val wordcountRDD = sparkContext.textFile("/datas/wordcount.data")
      .flatMap(line => line.split("\\s+"))
      .map(item => (item, 1))
      .reduceByKey((temp, item) => temp + item)

    wordcountRDD.foreach(println)

    println("=======================sortByKey=============================")
    wordcountRDD.map(tuple => tuple.swap)
      .sortByKey(ascending = false)
      .take(3)
      .foreach(println)

    println("=========================sortBy==============================")


    wordcountRDD.sortBy(tuple => tuple._2, ascending = false)
      .take(3)
      .foreach(println)

    println("===========================top===============================")
    wordcountRDD.top(3)
      .foreach(println)

    wordcountRDD.saveAsTextFile(s"/datas/sparkTopCount-${System.currentTimeMillis()}")
    sparkContext.stop()


  }

}
