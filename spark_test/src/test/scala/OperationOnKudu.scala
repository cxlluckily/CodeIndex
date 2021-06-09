
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.JavaConverters._


object OperationOnKudu {
  //DELETE
  def deleteKuduTable(kuduContext: KuduContext, tableName: String): Unit = {
    kuduContext.deleteTable(tableName)
  }

  //INSERT
  def insertKuduData(dataFrame: DataFrame, kuduContext: KuduContext, tableName: String): Unit = {
    kuduContext.insertRows(dataFrame, tableName)
  }

  //UPSERT
  def upsertKuduData(dataFrame: DataFrame, kuduContext: KuduContext, tableName: String): Unit = {
    kuduContext.upsertRows(dataFrame, tableName)
  }

  //UPDATE
  def updateKuduData(dataFrame: DataFrame, kuduContext: KuduContext, tableName: String): Unit = {
    kuduContext.updateRows(dataFrame, tableName)
  }


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val kuduDF: DataFrame = spark
      .read
      .options(Map("kudu.master" -> "192.168.88.161:7051", "kudu.table" -> "mos-uos_uos"))
      .format("kudu")
      .load()

    //    //DSL
    //    kuduDF.select("key").filter("key >= 5").show()
    //
    //SQL
    kuduDF.createOrReplaceTempView("kudu_table")
    spark.sql(
      """
        |SELECT *
        |FROM `kudu_table`
        |""".stripMargin)
      //打印10条看看
      .show(10, truncate = false)


    //创建表

    //    //可以使用已经有对的DataFrame来创建表
    val kuduContext = new KuduContext("node1:7051", spark.sparkContext)
    //    kuduContext.createTable("newTable", kuduDF.schema,
    //      Seq("key"),
    //      new CreateTableOptions()
    //        .setNumReplicas(1)
    //        .addHashPartitions(List("key").asJava, 3)
    //    )

    //也可以通过自定义schema来创建表
    val schema: StructType = StructType(
      Array(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = true)
      )
    )

    kuduContext.createTable("aNewTable", schema,
      Seq("id"),
      new CreateTableOptions()
        .setNumReplicas(1)
        .addHashPartitions(List("id").asJava, 3)
    )

    //
    //    //检查表是否存在
    //    kuduContext.tableExists("newTable")
    //
    //    //插入数据
    //    insertKuduData(kuduDF, kuduContext, "testTable")
    //    //    kuduContext.insertRows(kuduDF, "newTable")
    //
    //    //删除表
    //    deleteKuduTable(kuduContext, "testTable")
    //    kuduContext.deleteRows(kuduDF, "newTable")
    //
    //    //Upsert数据
    //    kuduContext.upsertRows(kuduDF, "newTable")
    //
    //    //更新数据
    //    val updateVal: Dataset[Row] = kuduDF.select($"key", ($"data" + 1).as("data")).limit(10)
    //    kuduContext.updateRows(updateVal, "newTable")
  }


}
