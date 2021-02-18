import org.apache.kudu.spark.kudu.{KuduContext, KuduDataFrameReader}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object deleteKuduData {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("deleteKudu")
      //设置Master_IP并设置spark参数
      .setMaster("local")
      .set("spark.worker.timeout", "500")
      .set("spark.cores.max", "10")
      .set("spark.rpc.askTimeout", "600s")
      .set("spark.network.timeout", "600s")
      .set("spark.task.maxFailures", "1")
      .set("spark.speculationfalse", "false")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkContext = SparkContext.getOrCreate(sparkConf)
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //使用spark创建kudu表
    val kuduMasters = "node1:7051,node2:7051,node3:7051"
    //TODO 1：定义kudu表
    val kuduTableName = "fox_tm_vehicle_series"

    //TODO 2：配置kudu参数
    val kuduOptions: Map[String, String] = Map(
      "kudu.table" -> kuduTableName,
      "kudu.master" -> kuduMasters)
//    import sqlContext.implicits._
//    //TODO 3：定义数据
//    val customers = Array(
//      Customer("jane", 30, "new york"),
//      Customer("jordan", 18, "toronto")
//    )
//
//    //TODO 4：创建RDD
//    val customersRDD = sparkContext.parallelize(customers)
//    //TODO 5：将RDD转成dataFrame
//    val customersDF = customersRDD.toDF()
//    //TODO 6：注册表
//    customersDF.registerTempTable("customers")
//
//    //TODO 7：编写SQL语句，过滤出想要的数据
//    val deleteKeysDF = sqlContext.sql("SELECT * FROM fox_tm_vehicle_series")
//
//    //TODO 8：使用kuduContext执行删除操作
//        kuduContext.deleteRows(deleteKeysDF, kuduTableName)
//    deleteKeysDF.show(10, truncate = false)
//
//    //TODO 9：查看kudu表中的数据
//    sqlContext.read.options(kuduOptions).kudu.show
  }
}
