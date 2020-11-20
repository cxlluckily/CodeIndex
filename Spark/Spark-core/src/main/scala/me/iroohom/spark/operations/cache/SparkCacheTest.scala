package me.iroohom.spark.operations.cache

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD中缓存函数，将数据缓存到内存或者磁盘，释放缓存
 */
object SparkCacheTest {
  def main(args: Array[String]): Unit = {
    val sc = {
      val sparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")

      new SparkContext(sparkConf)
    }


    val inputRDD = sc.textFile("datas\\wordcount.data", minPartitions = 2)

    /**
     * 将数据缓存至内存
     */
    inputRDD.cache()
    inputRDD.persist()

    /**
     * 使用Action函数触发缓存
     */
    println(s"Count = ${inputRDD.count()}")
    inputRDD.unpersist()

    /**
     * 缓存数据：选择缓存级别
     * val NONE = new StorageLevel(false, false, false, false)
     * val DISK_ONLY = new StorageLevel(true, false, false, false)
     * val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2)
     * val MEMORY_ONLY = new StorageLevel(false, true, false, true)
     * val MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2)
     * val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false)
     * val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)
     * val MEMORY_AND_DISK = new StorageLevel(true, true, false, true)
     * val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)
     * val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)
     * val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
     * val OFF_HEAP = new StorageLevel(true, true, true, false, 1)
     */

    inputRDD.persist(StorageLevel.MEMORY_AND_DISK)
    println(s"count: ${inputRDD.count()}")


    Thread.sleep(1000000)

    sc.stop()

  }

}
