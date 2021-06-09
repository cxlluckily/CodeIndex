package me.iroohom.externalSource

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 读取外部数据源
 */
object SparkSources {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      //TODO: 设置SparkSQL中产生时分区数目，实际项目中具体依据数据量和业务复杂度合适调整
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    import spark.implicits._

    /**
     * TODO:读取parquet格式的数据源
     * 在SparkSQL中，当加载读取文件数据时，如果不指定格式，默认是parquet格式数据
     */
    val parquetSource1: DataFrame = spark.read.format("parquet").load("D:\\roohom\\Spark\\Basic\\spark_day04_20201122\\05_数据\\resources\\users.parquet")
    val parquetSource: DataFrame = spark.read.parquet("D:\\roohom\\Spark\\Basic\\spark_day04_20201122\\05_数据\\resources\\users.parquet")

    //    parquetSource.printSchema()
    //    parquetSource.show(10, truncate = false)


    /**
     * TODO:读取文本格式的数据
     * 无论是 text 还是 textFile 加载文本数据时，字段名称：value, 类型String
     */
    val data: DataFrame = spark.read.text("D:\\roohom\\Spark\\Basic\\spark_day04_20201122\\05_数据\\resources\\people.txt")
    val dataset: Dataset[String] = spark.read.textFile("D:\\roohom\\Spark\\Basic\\spark_day04_20201122\\05_数据\\resources\\people.txt")
    //    data.printSchema()
    //    dataset.printSchema()

    /**
     * TODO:读取Json格式的数据
     */
    val jsonData: DataFrame = spark.read.json("D:\\roohom\\Spark\\Basic\\spark_day04_20201122\\05_数据\\resources\\employees.json")
    //    jsonData.printSchema()
    //    jsonData.show(5,truncate = false)

    /**
     * TODO:实际开发中，针对JSON格式文本数据，直接使用text/textFile读取，然后解析提取其中字段信息
     */
    val peopleDS: Dataset[String] = spark.read.textFile("D:\\roohom\\Spark\\Basic\\spark_day04_20201122\\05_数据\\resources\\employees.json")
    peopleDS.printSchema()
    peopleDS.show(10, truncate = false)
    //对JSON格式字符串，SparkSQL提供函数：get_json_object, def get_json_object(e: Column, path: String): Column
    import org.apache.spark.sql.functions.get_json_object
    val resultDF: DataFrame = peopleDS
      .select(
        //$.name $表示一整行数据，.name表示其中一个数据，如果name下还有数据，继续使用.访问
        get_json_object($"value", "$.name").as("name"),
        get_json_object($"value", "$.salary").cast(IntegerType).as("salary")
      )
    resultDF.printSchema()
    resultDF.show(10, truncate = false)

    spark.stop()
  }
}
