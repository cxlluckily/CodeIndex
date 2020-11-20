package me.iroohom.spark.operations.agg

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

import scala.collection.mutable.ListBuffer

/**
 * RDD中聚合函数，reduce、aggregate
 */
object SparkAggTest {
  def main(args: Array[String]): Unit = {
    val sc = {
      val sparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))

      new SparkContext(sparkConf)
    }

    /**
     * 模拟数据：从1 到 10 的列表，通过并行方式创建
     */

    val datasRDD = sc.parallelize(1 to 10, numSlices = 2)

    /**
     * 查看每个分区中的数据
     */
    datasRDD.foreachPartition {
      iter =>
        println(s"p-${TaskContext.getPartitionId()}: ${iter.mkString(", ")}")
    }

    println("=============================================================")

    /**
     * 使用reduce函数聚合
     */
    val result = datasRDD.reduce((temp, item) => {
      println(s"p-${TaskContext.getPartitionId()}: temp = ${temp}, item = ${item}")
      temp + item
    })
    println(result)
    println("=============================================================")

    /**
     * 使用fold函数聚合
     */
    val result1 = datasRDD.fold(0)((temp, item) => {
      println(s"p-${TaskContext.getPartitionId()}: temp = ${temp}, item = ${item}")
      temp + item
    })
    println(result1)
    println("=============================================================")

    /**
     * 使用aggregate函数获取最大的两个值
     */
    val top2 = datasRDD.aggregate(new ListBuffer[Int]())(
      /**
       * 分区内聚合 每个分区数据如何聚合 seqOp: (U,U) => U
       */
      (u, t) => {
        println(s"p-${TaskContext.getPartitionId()}: u = ${u} ,t = ${t}")

        u += t
        val top = u.sorted.takeRight(2)
        top
      },

      /**
       * 分区间聚合，每个聚合的结果如何聚合，combOp: (U,U) => U
       */
      (u1, u2) => {
        println(s"p-${TaskContext.getPartitionId()}: u = ${u1} ,t = ${u1}")
        u1 ++= u2
        u1.sorted.takeRight(2)
      }
    )
    println(top2)
  }
}
