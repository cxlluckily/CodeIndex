package me.iroohom.wordcount

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkDSLWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._


    val inputDS: Dataset[String] = spark.read.textFile("datas\\wordcount.data")
    inputDS.printSchema()
    inputDS.show(10)


    val resultDF: DataFrame = inputDS.filter(line => line != null && line.trim.length > 0)
      .flatMap(line => line.split("\\s+"))
      .groupBy("value")
      .count()

    resultDF.show(10)

    spark.stop()
  }
}
