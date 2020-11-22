package me.iroohom.spark.hanlp

import me.iroohom.spark.cases.SogouRecord
import org.apache.spark.{SparkConf, SparkContext}

object SogouPeriodCount {
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
              val logArr: Array[String] = log.trim.split("\\s+")
              SogouRecord(
                logArr(0), logArr(1), logArr(2).replaceAll("\\[|\\]", ""), //
                logArr(3).toInt, logArr(4).toInt, logArr(5) //
              )
          }
      }
    //    println(s"Count = ${recordsRDD.count()}, First = ${recordsRDD.first()}")

    /**
     * 搜索时间段统计
     */
    val resultRDD = recordsRDD
      .map {
        record =>
          record.queryTime.substring(0, 2)
      }
      .map {
        word => (word, 1)
      }
      //分组聚合
      .reduceByKey((temp, item) => temp + item)
      .sortBy(tuple => tuple._2, ascending = false)

    resultRDD.foreach(println)


    sc.stop()
  }
}