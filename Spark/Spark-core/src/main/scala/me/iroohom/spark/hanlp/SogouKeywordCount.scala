package me.iroohom.spark.hanlp

import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import me.iroohom.spark.cases.SogouRecord
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 搜狗搜索关键词统计
 */
object SogouKeywordCount {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val sparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))

      new SparkContext(sparkConf)
    }

    val inputRDD = sc.textFile("datas\\sogou\\SogouQ.reduced")

    /**
     * 过滤解析
     */
    val recordsRDD = inputRDD
      .filter(line => line != null && line.trim.split("\\s+").length == 6)
      .mapPartitions {
        iter =>
          iter.map {
            log =>
              val logArr = log.trim.split("\\s+")
              SogouRecord(
                logArr(0), logArr(1), logArr(2).replaceAll("\\[|\\]", ""), //
                logArr(3).toInt, logArr(4).toInt, logArr(5) //
              )
          }
      }
    //    println(s"Count = ${recordsRDD.count()}, First = ${recordsRDD.first()}")

    /**
     * 对搜索词进行中文分词
     */
    val wordsRDD = recordsRDD.mapPartitions {
      /**
       * 针对每个分区操作，每个分区的数据在一个迭代器里面
       */
      iter =>
        iter.flatMap {
          record =>

            /**
             * 使用HanLP进行中文分词
             */
            val terms: util.List[Term] = HanLP.segment(record.queryWords.trim)

            /**
             * 将Java的集合转换为scala的集合
             */
            import scala.collection.JavaConverters._
            terms.asScala.map(term => term.word)
        }
    }

    /**
     * 统计搜索词次数 取Top 10
     */

    val resultRDD = wordsRDD
      .map {
        word => (word, 1)
      }
      //分组聚合
      .reduceByKey((temp, item) => item + temp)
      //依据值进行排序
      .sortBy(tuple => tuple._2, ascending = false)
      .take(10)

    resultRDD.foreach(println)


    sc.stop()
  }
}
