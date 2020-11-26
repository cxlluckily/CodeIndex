package me.iroohom.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingTransformRDD {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = {
      val sparkConf = new SparkConf()
        //至少是2,最好是3
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      new StreamingContext(sparkConf, Seconds(5))
    }


    /**
     * def socketTextStream(
     * hostname: String,
     * port: Int,
     * storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
     * ): ReceiverInputDStream[String]
     */
    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream(
      "node1",
      9999,
      StorageLevel.MEMORY_AND_DISK
    )

    //能操作RDD就不要操作DStream 能操作RDD就操作RDD
    val resultDStream: DStream[(String, Int)] = inputDStream.transform { rdd =>
      val resultRDD: RDD[(String, Int)] = rdd
        .filter(line => line != null && line.trim.length > 0)
        .flatMap(line => line.split("\\s+"))
        .map(word => (word, 1))
        .reduceByKey((temp, item) => item + temp)

      resultRDD
    }

    resultDStream.print()


    //启动Receiver 接收器 Receiver划分流式数据的时间间隔BlockInterval ，默认值为 200ms，通过属性
    //【spark.streaming.blockInterval】设置
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
