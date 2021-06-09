import org.apache.kudu.client._
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.JavaConverters.asJavaIterableConverter


object ReadKuduAndCreate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("SparkReadKudu")
      .getOrCreate()

    import spark.implicits._
    // Read a table from Kudu
    val df = spark
      .read
      .options(
        Map(
          "kudu.master" -> "10.122.44.116:7051,10.122.44.117:7051,10.122.44.123:7051",
          "kudu.table" -> "sa_mos-pms-mysql-product_tm_product"
        ))
      .format("kudu")
      .load
//    df.show(10, truncate = false)

    //    println(df.count())

//    val kuduContext = new KuduContext(
//      "10.122.44.118:7051,10.122.44.119:7051,10.122.44.120:7051",
//      spark.sparkContext)
//
//
//    kuduContext.createTable("", df.schema, Seq("id"),
//      new CreateTableOptions().setNumReplicas(3).addHashPartitions(List("id").asJava, 3))


    df.createOrReplaceTempView("tab")

    spark.sql("SELECT * FROM tab LIMIT 10").show()

    spark.stop()

  }

}
