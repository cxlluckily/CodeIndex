package me.iroohom.logistics.offline.dws

import me.iroohom.logistics.common.{Configuration, OfflineTableDefine, SparkUtils}
import me.iroohom.logistics.offline.OfflineApp
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object ExpressBillDWS extends OfflineApp{
	/**
	 * 数据处理，包含加载宽表数据、依据指标进行分析和保存指标数据到DWS层
	 */
	override def execute(spark: SparkSession): Unit = {
		import spark.implicits._
		
		// step1. 加载宽表数据
		val expressBillDetailDF: DataFrame = getKuduSource(
			spark, OfflineTableDefine.EXPRESS_BILL_DETAIL, isLoadFullData = Configuration.IS_FIRST_RUNNABLE
		)
		// 由于多个指标分析，宽表数据被使用多次，需要缓存
		expressBillDetailDF.persist(StorageLevel.MEMORY_AND_DISK)
		
		// step2. 判断是全量还是增量，先以增量为准编写代码
		if(! Configuration.IS_FIRST_RUNNABLE){
			
			// step3. 按照业务指标计算
			// 指标一：计算总快递单数
			//expressBillDetailDF.count() // 返回值为Long类型
			val expressBillTotalDF: DataFrame = expressBillDetailDF.agg(count($"id").as("total"))
			
			// 指标二：各类客户快递单数，最大、最小、平均
			val expressBillTypeTotalDF: DataFrame = expressBillDetailDF.groupBy($"type").count()
			val expressBillTypeTotalAggDF: DataFrame = expressBillTypeTotalDF.agg(
				max($"count").as("maxTypeTotal"), //
				min($"count").as("minTypeTotal"), //
				round(avg($"count").as("avgTypeTotal"), 0) //
			)
			
			// 指标三：各网点快递单数，最大、最小、平均
			
			// 指标四：各渠道快递单数，最大、最小、平均
			
			// 指标五：各终端快递单数，最大、最小、平均
		}
	
	}
	
	def main(args: Array[String]): Unit = {
		// a. 构建SparkConf对象，设置应用相关配置信息
		val sparkConf: SparkConf = SparkUtils.autoSettingEnv(
			SparkUtils.sparkConf(this.getClass)
		)
		val spark: SparkSession = SparkUtils.getSparkSession(sparkConf)
		// b. 调用execute方法，加载源数据、处理转换数据和保存数据
		execute(spark)
		// c. 应用结束，关闭资源
		spark.stop()
	}
}
