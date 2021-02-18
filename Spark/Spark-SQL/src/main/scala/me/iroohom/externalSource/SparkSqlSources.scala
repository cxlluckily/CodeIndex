package me.iroohom.externalSource

import java.util.Properties

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlSources {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    import spark.implicits._

    //TODO:CSV 格式数据文本文件数据 -> 依据 CSV文件首行是否是列名称，决定读取数据方式不一样的
    val ratingsDF: DataFrame = spark.read
      //设置每行的字段间分割符,默认为逗号
      .option("sep", "\t")
      //设置数据文件行首为列名 默认为false
      .option("header", "true")
      //自动推荐数据类型，默认为false
      .option("inferSchema", "true")
      //指定文件的路径
      .csv("D:\\itcast\\Spark\\Basic\\spark_day04_20201122\\05_数据\\ml-100k\\u.dat")
    //    ratingsDF.printSchema()
    //    ratingsDF.show(10, truncate = false)


    //如果首行不是列信息，需要自定义Schema
    val schema = StructType(
      StructField("user_id", IntegerType, nullable = true) ::
        StructField("movie_id", IntegerType, nullable = true) ::
        StructField("rating", DoubleType, nullable = true) ::
        StructField("timestamp", LongType, nullable = true) :: Nil
    )

    val ratingSchemaDF: DataFrame = spark.read
      //设置分隔符
      .option("sep", "\t")
      //指定schema
      .schema(schema)
      .csv("D:\\itcast\\Spark\\Basic\\spark_day04_20201122\\05_数据\\ml-100k\\u.data")
    //    ratingSchemaDF.printSchema()
    //    ratingSchemaDF.show(10, truncate = false)

    /**
     * TODO:读取MySQL数据 不推荐
     */
    val props = new Properties()
    props.put("user", "root")
    props.put("password", "123456")
    props.put("driver", "com.mysql.cj.jdbc.Driver")


    val ratingsFromMysqlDF: DataFrame = spark.read
      .jdbc(
        "jdbc:mysql://localhost:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true", //
        "roohom.top10movies", //
        props
      )
    //    ratingsFromMysqlDF.printSchema()
    //    ratingsFromMysqlDF.show(10, truncate = false)

    /**
     * TODO：读取MySQL数据，标准格式写法
     */
    val sql =
      """
        |(SELECT
        |   movie_id as movie,
        |   rating ,
        |   `count`
        |FROM top10movies) as tmp
        |""".stripMargin

    val resultDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/roohom?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
      .option("user", "root")
      .option("password", "123456")

      /**
       * TODO：注意这里的写法,涉及到多表查询及嵌套查询可以使用如此
       */
      .option("dbtable", sql)
      .load()

    resultDF.printSchema()
    resultDF.show(10, truncate = false)

    spark.stop()
  }
}
