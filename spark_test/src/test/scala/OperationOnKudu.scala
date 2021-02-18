
import org.apache.kudu.client
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import collection.JavaConverters._


object OperationOnKudu {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .getOrCreate()

  import spark.implicits._

  val kuduDF: DataFrame = spark
    .read
    .options(Map("kudu-master" -> "node1:7051"))
    .format("kudu")
    .load()

  //DSL
  kuduDF.select("key").filter("key >= 5").show()

  //SQL
  kuduDF.createOrReplaceTempView("kudu_table")
  spark.sql(
    """
      |SELECT *
      |FROM `kudu_table`
      |WHERE key >= 5
      |""".stripMargin)
    //打印10条看看
    .show(10, truncate = false)


  //创建表

  //可以使用已经有对的DataFrame来创建表
  private val kuduContext = new KuduContext("kudu.master:7051", spark.sparkContext)
  kuduContext.createTable("newTable", kuduDF.schema,
    Seq("key"),
    new CreateTableOptions()
      .setNumReplicas(1)
      .addHashPartitions(List("key").asJava, 3)
  )

  //也可以通过自定义schema来创建表
  private val schema: StructType = StructType(
    StructField("id", IntegerType, nullable = true) :: Nil
  )

  kuduContext.createTable("aNewTable", schema,
    Seq("key"),
    new CreateTableOptions()
      .setNumReplicas(1)
      .addHashPartitions(List("key").asJava, 3)
  )


  //检查表是否存在
  kuduContext.tableExists("newTable")

  //插入数据
  kuduContext.insertRows(kuduDF, "newTable")

  //删除数据
  kuduContext.deleteRows(kuduDF, "newTable")

  //Upsert数据
  kuduContext.upsertRows(kuduDF, "newTable")

  //更新数据
  private val updateVal: Dataset[Row] = kuduDF.select($"key", ($"data" + 1).as("data")).limit(10)
  kuduContext.updateRows(updateVal, "newTable")

  //删除表
  kuduContext.deleteTable("newTable")

}
