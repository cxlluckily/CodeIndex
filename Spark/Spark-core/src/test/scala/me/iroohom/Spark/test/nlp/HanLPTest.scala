package me.iroohom.Spark.test.nlp

import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.tokenizer.StandardTokenizer

import scala.collection.mutable

/**
 * HanLP中文分词测试
 */
object HanLPTest {
  def main(args: Array[String]): Unit = {
    val terms: util.List[Term] = HanLP.segment("杰克奥特曼打怪兽")

    //TODO:在scala中遍历访问Java的集合，是无法做到的 必须转化为scala的集合才可以访问
    import scala.collection.JavaConverters._
    val list: mutable.Seq[Term] = terms.asScala
    list.foreach(item => println(item.word))


    //标准分词
    val terms1: util.List[Term] = StandardTokenizer.segment("端午++重阳++粽子")



  }
}
