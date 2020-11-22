package me.iroohom.wordcount

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSQLWordcount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()


    import spark.implicits._

    val inputDS: Dataset[String] = spark.read.textFile("datas\\wordcount.data")


    val wordsDS: Dataset[String] = inputDS
      .filter(line => line != null && line.trim.length > 0)
      .flatMap(line => line.split("\\s+"))

    wordsDS.createOrReplaceTempView("view_tmp_words")
    val resultDF: DataFrame = spark.sql(
      """
			  |SELECT value AS word, COUNT(1) AS count FROM view_tmp_words GROUP BY value
			  |""".stripMargin
    )


    resultDF.show(10)


    spark.stop()
  }
}
