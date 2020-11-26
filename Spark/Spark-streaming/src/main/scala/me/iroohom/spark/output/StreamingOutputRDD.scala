package me.iroohom.spark.output

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object StreamingOutputRDD {
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

    //定义数据终端，将每批次的结果数据输出
    //    resultDStream.print()
    resultDStream.foreachRDD((rdd, time) => {
      //时间转换
      //val xx: Time = time
      val format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
      println()
      println("+=====================================================+")
      println(s"Time: ${format.format(new Date(time.milliseconds))}")
      println("+=====================================================+")


      //如果RDD没有数据就不打印
      if (!rdd.isEmpty()) {
        rdd.coalesce(1)
          .foreachPartition { iter =>
            iter.foreach { item => println(item) }
          }
        //保存数据到HDFS文件
        rdd.coalesce(1).saveAsTextFile(s"datas/spark/spark-wc-${System.currentTimeMillis()}")
      }
    })

    //启动Receiver 接收器 Receiver划分流式数据的时间间隔BlockInterval ，默认值为 200ms，通过属性
    //【spark.streaming.blockInterval】设置
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
