package me.iroohom.spark.start

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    //构建StreamingContext流式上下文实例对象
    val ssc: StreamingContext = {
      val sparkConf = new SparkConf()
        //至少是2,最好是3
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))

      //时间间隔，用于划分流式数据为很多批次
      new StreamingContext(sparkConf, Seconds(5))
    }


    /**
     * def socketTextStream(
     * hostname: String,
     * port: Int,
     * storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
     * ): ReceiverInputDStream[String]
     */
    val inputDStream1: ReceiverInputDStream[String] = ssc.socketTextStream(
      "node1",
      9999,
      StorageLevel.MEMORY_AND_DISK
    )

    val inputDStream2: ReceiverInputDStream[String] = ssc.socketTextStream(
      "node1",
      8888,
      StorageLevel.MEMORY_AND_DISK
    )

    val inputDStream = inputDStream1.union(inputDStream2)


    /**
     * 对每批次的数据做词频统计
     */
    val resultDS: DStream[(String, Int)] = inputDStream
      .filter(line => line != null && line.trim.length > 0)
      .flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey((temp, item) => item + temp)

    resultDS.print()


    //启动Receiver 接收器 Receiver划分流式数据的时间间隔BlockInterval ，默认值为 200ms，通过属性
    //【spark.streaming.blockInterval】设置
    ssc.start()
    // 流式应用启动以后，正常情况一直运行（接收数据、处理数据和输出数据），除非人为终止程序或者程序异常停止
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
