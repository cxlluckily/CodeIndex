package me.iroohom.WordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark框架案例：使用Spark做词频统计
 */
object SparkWordCount {
  def main(args: Array[String]): Unit = {

    //TODO: Spark应用程序中，入口为SparkContext,必须创建实例对象，加载数据和调度程序执行
    //创建SparkConf对象，设置应用相关信息，比如master
    val sparkConf = new SparkConf()
      //SparkWordCount$ 转化为 SparkWordCount
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[2]")

    //构建SparkContext实例对象，传递SparkConf
    val sparkContext = new SparkContext(sparkConf)


    //TODO: 读取数据 封装RDD集合 读取数据和调度Job执行
    val inputRDD: RDD[String] = sparkContext.textFile("/datas/wordcount.data")

    //TODO:处理数据 调用RDD中函数
    val wordsRDD = inputRDD.flatMap(line => line.split("\\s+"))

    //TODO: 转换为二元组
    val tuplesRDD = wordsRDD.map(item => (item, 1))
    //TODO: 按照单词分组，按照值聚合
    val wordcountRDD = tuplesRDD.reduceByKey((temp, item) => item + temp)

    //打印
    wordcountRDD.foreach(item => println(item))
    //保存成文件
    wordcountRDD.saveAsTextFile(s"/datas/sparkWc-${System.currentTimeMillis()}")

    Thread.sleep(100000)

    sparkContext.stop()


  }
}
