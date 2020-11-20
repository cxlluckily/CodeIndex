package me.iroohom.spark.operations.iter

import org.apache.spark.{SparkConf, SparkContext, TaskContext}


/**
 * 分区操作函数 mapPartitions和foreachPartition
 */

object SparkIterTest {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val sparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))

      new SparkContext(sparkConf)
    }

    /**
     * 从文件系统加载数据 创建RDD数据集
     */
    val inputRDD = sc.textFile("datas\\wordcount.data", minPartitions = 2)

    val wordcountRDD = inputRDD.flatMap(line => line.split("\\s+"))

      /**
       * 针对每个分区进行操作
       */
      .mapPartitions {
        iter =>

          /**
           * 每个RDD中每个分区中的数据存储在迭代器中，相当于列表List
           */
          iter.map(word => (word, 1))
      }
      .reduceByKey((a, b) => a + b)

    /**
     * 输出结果到本地文件系统
     */
    wordcountRDD.foreachPartition { datas =>
      val partitionId: Int = TaskContext.getPartitionId()

      datas.foreach{case (word,count)=>
      println(s"p-${partitionId}: word = $word, count = $count")
      }
    }

    sc.stop()


  }
}
