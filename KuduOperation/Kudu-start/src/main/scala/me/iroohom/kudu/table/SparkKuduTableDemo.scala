package me.iroohom.kudu.table

import java.util
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/**
 * 使用KuduContext创建Kudu中的表
 */
object SparkKuduTableDemo {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    /**
     * 创建kuduContext对象，传递SparkContext实例
     */
    val kuduContext = new KuduContext("node2:7051", spark.sparkContext)

    /**
     * 设置表的属性 定义Schema
     */
    val schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType, nullable = true)
      .add("age", IntegerType, nullable = true)
      .add("gender", StringType, nullable = true)

    /**
     * 定义Keys
     */
    val keys: Seq[String] = Seq("id")
    /**
     * 定义TableOptions
     */
    val tableOptions = new CreateTableOptions()
    val columns: util.ArrayList[String] = new util.ArrayList[String]()
    columns.add("id")
    tableOptions.addHashPartitions(columns, 3)
    tableOptions.setNumReplicas(1)

    //创建表
    val kuduTable = kuduContext.createTable("new-users", schema, keys, tableOptions)
    println(s"Kudu Table ID is ${kuduTable.getTableId}")


    spark.stop()
  }
}
