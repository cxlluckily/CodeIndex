package me.iroohom.udf

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUdfTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    import spark.implicits._

    val empDF: DataFrame = spark.read.json("D:\\itcast\\Spark\\Basic\\spark_day04_20201122\\05_数据\\resources\\employees.json")

    //    empDF.printSchema()
    //    empDF.show(10, truncate = false)

    /**
     * 自定义UDF函数功能：
     * 将某个列数据，转换为大写
     */
    spark.udf.register(
      "lower_case",
      //匿名函数
      (name: String) => {
        name.toLowerCase
      }
    )

    //注册临时视图
    empDF.createOrReplaceTempView("view_tmp_emp")

    spark.sql(
      """
        |SELECT
        |name,
        |lower_case(name) as lowerName
        |FROM view_tmp_emp
        |""".stripMargin).show(10, truncate = false)

    println("====================================================")

    /**
     * 在DSL中使用
     */
    import org.apache.spark.sql.functions.udf
    val udfLower = udf(
      (name: String) => {
        name.toLowerCase()
      }
    )

    empDF.select(
      $"name",
      udfLower($"name").as("lowerName")
    ).show(10, truncate = false)

    spark.stop()
  }
}
