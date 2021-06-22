package me.iroohom.kudu.data

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 使用SparkSession读取Kudu数据，封装到DF/DS中
 */
object SparkSQLKuduDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    import spark.implicits._


    /**
     * 从Kudu加载数据
     */

    val inputDF: DataFrame = spark.read
      .format("kudu")
      .option("kudu.master", "node2:7051")
      .option("kudu.table", "new-users")
      .load()

    /**
     * 自定义UDF函数实现将用户性别转换 男 -> M , 女 -> F
     */
    val gender_udf: UserDefinedFunction = udf(
      (gender: String) => {
        if (gender.equals("男")) "M" else "F"
      }
    )

    val resultDF: DataFrame = inputDF.select(
      $"id", $"name", $"age", gender_udf($"gender").as("gender")
    )

    resultDF.write
      //m目前DF数据保存至Kudu表时，仅仅支持Append模式
      .mode(SaveMode.Append)
      .format("kudu")
      .option("kudu.master", "node2:7051")
      .option("kudu.table", "new-users")
      .save()
    spark.stop()
  }
}
