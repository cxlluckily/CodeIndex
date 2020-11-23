package me.iroohom.hbase

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 使用Hbase工具类(me.iroohom.sql)从Hbase读取数据
 */
object SparkHbaseSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    /**
     * // 连接HBase数据库的属性名称
     * val HBASE_ZK_QUORUM_KEY: String = "hbase.zookeeper.quorum"
     * val HBASE_ZK_QUORUM_VALUE: String = "zkHosts"
     * val HBASE_ZK_PORT_KEY: String = "hbase.zookeeper.property.clientPort"
     * val HBASE_ZK_PORT_VALUE: String = "zkPort"
     * val HBASE_TABLE: String = "hbaseTable"
     * val HBASE_TABLE_FAMILY: String = "family"
     * val SPERATOR: String = ","
     * val HBASE_TABLE_SELECT_FIELDS: String = "selectFields"
     * val HBASE_TABLE_ROWKEY_NAME: String = "rowKeyColumn"
     */
    val hbaseDF: DataFrame = spark.read.format("hbase")
      .option("zkHosts", "node1")
      .option("zkPort", "2181")
      .option("hbaseTable", "htb_wordcount")
      .option("family", "info")
      .option("selectFields", "count")
      .load()

    hbaseDF.printSchema()
    hbaseDF.show(10, truncate = false)

    spark.stop()

  }
}
