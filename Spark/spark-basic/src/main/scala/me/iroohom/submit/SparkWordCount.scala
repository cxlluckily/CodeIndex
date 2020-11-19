package me.iroohom.submit

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark框架案例：使用Spark做词频统计 打包运行：已测试
 */
object SparkWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: SparkSubmit <input> <output>")
      System.exit(1)
    }

    //TODO: Spark应用程序中，入口为SparkContext,必须创建实例对象，加载数据和调度程序执行
    //创建SparkConf对象，设置应用相关信息，比如master
    val sparkConf = new SparkConf()
      //SparkWordCount$ 转化为 SparkWordCount
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[2]")

    //构建SparkContext实例对象，传递SparkConf
    val sparkContext = new SparkContext(sparkConf)


    //TODO: 读取数据 封装RDD集合 读取数据和调度Job执行
    val inputRDD: RDD[String] = sparkContext.textFile(s"${args(0)}")

    //TODO:处理数据 调用RDD中函数
    val wordsRDD = inputRDD.flatMap(line => line.split("\\s+"))

    //TODO: 转换为二元组
    val tuplesRDD = wordsRDD.map(item => (item, 1))
    //TODO: 按照单词分组，按照值聚合
    val wordcountRDD = tuplesRDD.reduceByKey((temp, item) => item + temp)

    //打印
    wordcountRDD.foreach(item => println(item))
    //保存成文件 冒号不能作为HDFS文件名
    val dateFormat = new SimpleDateFormat("yyyyMMdd-HH-mm-ss")
    val date = dateFormat.format(System.currentTimeMillis())
    wordcountRDD.saveAsTextFile(s"${args(1)}-${date}")

    //    Thread.sleep(100000)

    sparkContext.stop()

  }
}
