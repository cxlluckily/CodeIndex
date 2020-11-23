package me.iroohom.externalSource

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSQLHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "4")
      .enableHiveSupport()
      .config("hive.metastore.uris", "thrift://node1.itcast.cn:9083")
      .getOrCreate()

    import spark.implicits._

    /**
     * DSL方式
     */
    spark.read
      .table("db_hive.emp")
      .groupBy($"deptno")
      .agg(round(avg($"sal"),2).as("avg_sal"))
      .show(10, truncate = false)

    println("===============================================================")

    /**
     * sql方式
     */
    spark.sql(
      """
        |SELECT
        |   deptno,
        |   round(avg(sal),2) as avg_sal
        |FROM
        |   db_hive.emp
        |GROUP BY
        |   deptno
        |""".stripMargin)
      .show(10, truncate = false)


    spark.stop()
  }
}
