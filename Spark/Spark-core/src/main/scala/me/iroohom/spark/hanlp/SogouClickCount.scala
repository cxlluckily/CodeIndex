package me.iroohom.spark.hanlp

import me.iroohom.spark.cases.SogouRecord
import org.apache.spark.{SparkConf, SparkContext}

object SogouClickCount {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val sparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))

      new SparkContext(sparkConf)
    }

    val inputRDD = sc.textFile("datas\\sogou\\SogouQ.reduced")

    /**
     * 过滤解析
     */
    val recordsRDD = inputRDD
      .filter(line => line != null && line.trim.split("\\s+").length == 6)
      .mapPartitions {
        iter =>
          iter.map {
            log =>
              val logArr = log.trim.split("\\s+")
              SogouRecord(
                logArr(0), logArr(1), logArr(2).replaceAll("\\[|\\]", ""), //
                logArr(3).toInt, logArr(4).toInt, logArr(5) //
              )
          }
      }
    //    println(s"Count = ${recordsRDD.count()}, First = ${recordsRDD.first()}")

    println("================================用户点击率统计================================")
    val resultRDD = recordsRDD
      .map {
        record =>
          //将搜索用户的id和搜索词组合成一个元组作为键，值设置为1
          val key = record.userId -> record.queryWords
          (key, 1)
      }
      //分组聚合
      .reduceByKey((temp, item) => temp + item)

    /**
     * 取前10的结果
     */
    val clickCountRDD = resultRDD.sortBy(tuple => tuple._2, ascending = false)
      .take(10)

    clickCountRDD.foreach(println)
    println(s"Max Click Count = ${resultRDD.map(_._2).max()}")
    println(s"Min Click Count = ${resultRDD.map(_._2).min()}")
    println(s"Mean Click Count = ${resultRDD.map(_._2).mean()}")


    sc.stop()
  }
}
