package com.study.spark.streaming

import com.stockemotion.common.utils.{JsonUtils, ObjectUtils}
import com.study.spark.config.{ConfigurationManager, StockRedisConstants}
import com.study.spark.pool.RedisStockInfoClient
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by piguanghua on 2017/10/16.
  */
object StockWodeSignalCalculateDemo {
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("StockWodeSignalCalculate").setMaster("local[2]")

		// .setMaster("local[2]")
		val ssc = new StreamingContext(sparkConf, Seconds(6))

		ssc.checkpoint("D:\\Data\\checkpoint")

		//	val paras = Array("192.168.152.137:9092,192.168.152.160:9092,192.168.152.163:9092", "incDownStop")
		val paras = Array("spark1:9092,spark2:9092,spark3:9092", "wodeSignal")
		val Array(brokers, topics) = paras
		val topicsSet = topics.split(",").toSet
		val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
		val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
			ssc, kafkaParams, topicsSet)
		/*
				val stockInfoMap = scala.collection.mutable.Map[String, String]()
				//缓存stock数据
				//需要checkpoint
				val redisStockInfo = RedisStockInfoClient.getResource()
				val stockCodeString = redisStockInfo.lrange(StockRedisConstants.STOCK_ALL_CODE, 0, -1);

				for(jsonString <-  stockCodeString){
					import com.stockemotion.common.utils.JsonUtils
					val jsonObject = JsonUtils.TO_JSONObject(jsonString)
					val stockCode = ObjectUtils.toString(jsonObject.get("stock_code"))
					val stockName = ObjectUtils.toString(jsonObject.get("stock_name"))
					stockInfoMap += (stockCode->stockName)
				}
		RedisStockInfoClient.releaseResource(redisStockInfo)
*/
		//收集数据
		val event = messages.flatMap(line => {
			val data = JsonUtils.TO_JSONObject(line._2)
			Some(data)
		})

		val result =
		messages
		  .map(line =>
			  {
				  val data = JsonUtils.TO_JSONObject(line._2)
				  StockWdState(ObjectUtils.toString(data.get("stockCode")), ObjectUtils.toInteger(data.get("wodeSignal")))
				  ( ObjectUtils.toString(data.get("stockCode")) ,  ObjectUtils.toInteger(data.get("wodeSignal"))   )
				//  (line,1)
			  })
		  .reduceByKeyAndWindow(
			  (oldElement,newElement)=>{   //key值相同，如何过滤

			//	  if(oldElement._1 > newElement._2)
				  oldElement
			  },
			(a,b) => a
			,Seconds(12),Seconds(6))


		result.print()



		ssc.start()
		ssc.awaitTermination()
	}



}
