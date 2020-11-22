package me.iroohom.spark.SharedVariable

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object SparkSharedVariableTest {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val sparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("&"))
      new SparkContext(sparkConf)
    }

    val inputRDD = sc.textFile("datas\\filter\\datas.input", minPartitions = 2)
    // TODO: 字典数据，只要有这些单词就过滤: 特殊字符存储列表List中
    val list: List[String] = List(",", ".", "!", "#", "$", "%")
    // TODO: 通过广播变量 将列表list广播到各个Executor内存中，便于多个Task使用
    val listBroadcast: Broadcast[List[String]] = sc.broadcast(list)

    // TODO: 定义累加器，记录单词为符号数据的个数
    val accumulator: LongAccumulator = sc.longAccumulator("number_accum")

    /**
     * 分割单词 过滤数据 wordcount
     */
    val resultRDD = inputRDD
      .filter {
        line =>
          if (line != null && line.trim.length > 0) {
            true
          }
          else {
            false
          }}
      .flatMap {
        line => line.trim.split("\\s+")
      }
      .filter {
        word =>
          if (listBroadcast.value.contains(word)) {
            accumulator.add(1L)
            false
          } else {
            true
          }}.map {
      word => (word, 1)
    }.reduceByKey((temp, itemp) => temp + itemp)

    //打印一下看看
    resultRDD.foreach(println)

    println(s"counter = ${accumulator.value}")


    sc.stop()
  }
}
