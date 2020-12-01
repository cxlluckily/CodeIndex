package me.iroohom.spark.operations.join

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD中关联函数Join，针对RDD中数据类型为Key/Value对
 */
object SparkJoinTest {
  def main(args: Array[String]): Unit = {
    val sc = {
      val sparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))

      new SparkContext(sparkConf)
    }

    val empRDD = sc.parallelize(
      Seq(
        (1001, "zhangsan"),
        (1002, "lisi"),
        (1003, "wangwu"),
        (1004, "zhaoliu")
      )
    )

    val deptRDD = sc.parallelize(
      Seq(
        (1001, "sales"),
        (1002, "tech")
      )
    )

    //VITAL: 面试：def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))]
    val joinRDD = empRDD.join(deptRDD)
    joinRDD.foreach(println)


    val leftJoinRDD: RDD[(Int, (String, Option[String]))] = empRDD.leftOuterJoin(deptRDD)
    leftJoinRDD.foreach {
      case (deptNo, (empName, option)) =>
        val deptName = option match {
          case Some(value) => value
          case None => "None"
        }
        println(s"deptNo = ${deptNo}, empName = ${empName}, deptName = ${deptName}")
    }


    sc.stop()
  }
}
