package me.iroohom.kudu.data

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Kudu集成Spark，使用KuduContext对Kudu表中的数据进行操作
 */
object SparkKuduDataDemo {

  /**
   * 向Kudu表中插入数据
   *
   * @param sparkSession spar会话对象
   * @param context      上下文
   * @param table        表名
   */
  def insertData(sparkSession: SparkSession, context: KuduContext, table: String): Unit = {
    val dataframe: DataFrame = sparkSession.createDataFrame(
      Seq(
        (10001, "jack", 23, "男"),
        (10002, "mike", 21, "女"),
        (10003, "nike", 22, "女"),
        (10004, "puma", 35, "男"),
        (10005, "kappa", 36, "女")
      )
    ).toDF("id", "name", "age", "gender")

    context.insertRows(dataframe, table)
  }


  /**
   * 查询数据
   *
   * @param sparkSession spar会话对象
   * @param context      上下文
   * @param table        表名
   */
  def queryData(sparkSession: SparkSession, context: KuduContext, table: String): Unit = {
    val usersRDD: RDD[Row] = context.kuduRDD(sparkSession.sparkContext, table, Seq("id", "name", "age"))

    usersRDD.foreach { row =>
      val id = row.getInt(0)
      val name = row.getString(1)
      val age = row.getInt(2)
      println(s"id is ${id}, name is ${name} , age is ${age}")
    }
  }

  def updateData(spark: SparkSession, context: KuduContext, table: String): Unit = {

    // TODO: 当时KuduContext操作表示，进行C、U、D操作时，传递数据封装在DataFrame中
    //context.updateRows(dataframe, table)
    //context.upsertRows(dataframe, table)
    //context.deleteRows(dataframe, table)
  }


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    import spark.implicits._

    //创建KuduContext对象
    val kuduContext = new KuduContext("node2:7051", spark.sparkContext)

    val tableName = "new-users"


    // 插入数据
    //    insertData(spark, kuduContext, tableName)

    // 查询数据
    queryData(spark, kuduContext, tableName)

    // 更新数据
    //    updateData(spark, kuduContext, tableName)

    // 插入更新数据
    //upsertData(spark, kuduContext, tableName)

    // 删除数据
    //deleteData(spark, kuduContext, tableName)

    spark.stop()
  }
}
