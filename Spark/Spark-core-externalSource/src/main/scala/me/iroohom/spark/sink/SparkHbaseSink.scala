package me.iroohom.spark.sink

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将RDD表中数据Sink到Hbase
 */
object SparkHbaseSink {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val sparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")

      new SparkContext(sparkConf)
    }

    /**
     * 构建RDD
     */

    val inputRDD = sc.textFile("datas\\wordcount.data", minPartitions = 2)

    val resultRDD = inputRDD
      .flatMap({ line => line.trim.split("\\s+") })
      .map { word => (word, 1) }
      .reduceByKey((temp, item) => temp + item)

    //    resultRDD.foreach(println)


    // TODO: 将数据写入到HBase表中, 使用saveAsNewAPIHadoopFile函数，要求RDD是(key, Value)
    // TODO: 组装RDD[(ImmutableBytesWritable, Put)]
    /**
     * HBase表的设计：
     * 表的名称：htb_wordcount
     * Rowkey: word
     * 列簇: info
     * 字段名称： count
     */
    val putsRDD = resultRDD
      .map {
        case (word, count) => {
          /**
           * 构建Put对象
           */
          val wordBytes = Bytes.toBytes(word)
          val rowKey = new ImmutableBytesWritable(wordBytes)
          val put = new Put(wordBytes)
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("count"), Bytes.toBytes(count.toString))

          rowKey -> put
        }
      }

    //TODO:调用saveAsNewAPIHadoopFile保存数据到Hbase
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "node1.roohom.cn")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent", "/hbase")
    // 设置将数据保存的HBase表的名称
    conf.set(TableOutputFormat.OUTPUT_TABLE, "htb_wordcount")

    /**
     * def saveAsNewAPIHadoopFile(
     * path: String,
     * keyClass: Class[_],
     * valueClass: Class[_],
     * outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
     * conf: Configuration = self.context.hadoopConfiguration
     * ):Unit
     */
    putsRDD.saveAsNewAPIHadoopFile(
      "datas/spark/hbase/write-wordcount"+ System.currentTimeMillis(),
      classOf[ImmutableBytesWritable],
      classOf[Put],
      classOf[TableOutputFormat[ImmutableBytesWritable]],
      conf
    )


    sc.stop()
  }
}
