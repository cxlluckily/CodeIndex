package me.iroohom.spark.source

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkReadHbase {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val sparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        //TODO:设置Keyo序列化方式
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        //TODO:注册序列化的数据类型
        .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result]))
      new SparkContext(sparkConf)
    }


    val conf = HBaseConfiguration.create()
    // 设置连接Zookeeper属性
    conf.set("hbase.zookeeper.quorum", "node1.itcast.cn")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent", "/hbase")
    // 设置将数据保存的HBase表的名称
    conf.set(TableInputFormat.INPUT_TABLE, "htb_wordcount")

    /**
     * def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
     * conf: Configuration = hadoopConfiguration,
     * fClass: Class[F],
     * kClass: Class[K],
     * vClass: Class[V]
     * ): RDD[(K, V)]
     */
    val readRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    println(s"count = ${readRDD.count()}")


    /**
     * 打印一下看看
     */
    readRDD.foreach {
      case (rowkey, result) =>
        val cells = result.rawCells()
        cells.foreach { cell =>
          val family = Bytes.toString(CellUtil.cloneFamily(cell))
          val column = Bytes.toString(CellUtil.cloneQualifier(cell))
          val value = Bytes.toString(CellUtil.cloneValue(cell))

          println(
            //s"key = ${Bytes.toString(rowkey.get())}, " +  // TODO: 此行代码有问题，底层迭代器有BUG
            s"key = ${Bytes.toString(result.getRow)},\t" + s"column = ${family}:${column},\t" + s"timestamp = ${cell.getTimestamp},\t" + s"value = ${value}"
          )
        }
    }


    sc.stop()
  }
}
