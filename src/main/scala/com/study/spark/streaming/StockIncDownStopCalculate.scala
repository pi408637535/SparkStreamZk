package com.study.spark.streaming

import java.util

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.stockemotion.common.utils.{DateUtils, JsonUtils, ObjectUtils}
import com.study.spark.config.{PushRedisConstants, StockRedisConstants}
import com.study.spark.pool.{RedisStockInfoClient, RedisStockPushClient}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.JavaConversions._
import com.study.spark.config.PushRedisConstants
import com.study.spark.streaming.mysql.{MDBManager, SparkPushConnectionPool}
import com.study.spark.utils.{PushUtils, StringUtils, TimeUtils, WodeInfoUtils}
import org.apache.commons.collections.CollectionUtils
/**
  * Created by piguanghua on 2017/9/13.
  */

object StockIncDownStopCalculate {
	def main(args: Array[String]): Unit = {
		// Create context with 2 second batch interval
		val sparkConf = new SparkConf().setAppName("StockIncDownStopCalculate").setMaster("spark://spark1:7077")// .setMaster("local[2]")
		val ssc = new StreamingContext(sparkConf, Seconds(3))

	//	val paras = Array("192.168.152.137:9092,192.168.152.160:9092,192.168.152.163:9092", "incDownStop")
	    val paras = Array("192.168.1.226:9092,192.168.1.161:9092,192.168.1.227:9092", "incDownStopNew")
		val Array(brokers, topics) = paras
		val topicsSet = topics.split(",").toSet
		val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
		val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
			ssc, kafkaParams, topicsSet)

		val stockInfoMap = scala.collection.mutable.Map[String, String]()
		//缓存stock数据
		//需要checkpoint
		val redisStockInfo = RedisStockInfoClient.getResource()
		val stockCodeString = redisStockInfo.lrange(StockRedisConstants.STOCK_ALL_PAN_CODE, 0 ,-1);
		for(jsonString <-  stockCodeString){
			import com.stockemotion.common.utils.JsonUtils
			val jsonObject = JsonUtils.TO_JSONObject(jsonString)
			val stockCode = ObjectUtils.toString(jsonObject.get("stock_code"))
			val stockName = ObjectUtils.toString(jsonObject.get("stock_name"))
			stockInfoMap += (stockCode->stockName)
		}
		RedisStockInfoClient.releaseResource(redisStockInfo)

		//收集数据
		val event=messages.flatMap(line => {
			val data = JsonUtils.TO_JSONObject(line._2)
			Some(data)
		})

		event.foreachRDD(jsonArray =>{

			jsonArray.foreachPartition(iterator=>{
				val connPush = MDBManager.getMDBManager.getConnection

				val redisStockPushClient = RedisStockPushClient.pool.getResource()

				iterator.foreach(iteratorElement=>{



					//获取redis push pool
					//解析股票数据
					val stockPriceClose = iteratorElement.get("stockPriceClose").toString.toDouble
					val stockPriceHigh = iteratorElement.get("stockPriceHigh").toString.toDouble
					val stockPriceLow = iteratorElement.get("stockPriceLow").toString.toDouble
					val stockPriceYesterday = iteratorElement.get("stockPriceYesterday").toString.toDouble
					val stockCode = iteratorElement.get("stockCode").toString

					val stockName = stockInfoMap.get(iteratorElement.get("stockCode").toString).get
					val stockCodeUsual = stockCode.substring(0, stockCode.lastIndexOf("."))
					val stockPercent = StringUtils.formatDouble( (stockPriceClose - stockPriceYesterday) / stockPriceYesterday * 100 )


					if(stockPriceClose >= stockPriceHigh ){ //涨停

						val userSet = redisStockPushClient.smembers(PushRedisConstants.STOCK_PUSH_ELF_INC_DROP_STOP_USER_SET   + stockCode )

						userSet.foreach(userId=>{

							val content = getUpStopMessage(stockName, stockCodeUsual, stockPriceClose, stockPercent)
							val pushMessage = StockIncDownStopCalculate.getUpStopMessage(stockName, stockPercent, stockPriceClose, stockPercent)
							val deviceType = ObjectUtils.toInteger(redisStockPushClient.get(PushRedisConstants.STOCK_PUSH_USER_DEVICETYPE + userId)).byteValue

							PushUtils.sendElfPushMessage(stockCodeUsual, stockName, content, redisStockPushClient.get(PushRedisConstants.STOCK_PUSH_USER_CLIENTID + userId), pushMessage, deviceType)

							redisStockPushClient.srem(PushRedisConstants.STOCK_PUSH_ELF_INC_DROP_STOP_USER_SET + stockCode, userId)

							val wodeInfo = new JSONObject()
							wodeInfo.put("stockCode", stockCodeUsual)
							wodeInfo.put("stockName", stockName)
							wodeInfo.put("content", content)
							WodeInfoUtils.message(userId, "涨跌停推送", content, wodeInfo)


							val sqlPush = "insert into push_log(stock_code,user_id,inc_drop_stop,percent_now,sys_create_time) "+ "values('"  + stockCode +"'" + "," + userId + "," + 1  + ","+ stockPercent + ","+    "'" + TimeUtils.getCurrent_time() +"'" + ")"
							val stmtPush = connPush.createStatement()
							stmtPush.execute(sqlPush)

						})
					}
					if(stockPriceClose <= stockPriceLow ){
						val userSet = redisStockPushClient.smembers(PushRedisConstants.STOCK_PUSH_ELF_INC_DROP_STOP_USER_SET   + stockCode )

						userSet.foreach(userId=>{
							val content = getDownStopMessage(stockName, stockCodeUsual, stockPriceClose, stockPercent)
							val pushMessage = StockIncDownStopCalculate.getUpStopMessage(stockName, stockPercent, stockPriceClose, stockPercent)
							val deviceType = ObjectUtils.toInteger(redisStockPushClient.get(PushRedisConstants.STOCK_PUSH_USER_DEVICETYPE + userId)).byteValue
							PushUtils.sendElfPushMessage(stockCodeUsual, stockName, content, redisStockPushClient.get(PushRedisConstants.STOCK_PUSH_USER_CLIENTID + userId), pushMessage, deviceType)

							redisStockPushClient.srem(PushRedisConstants.STOCK_PUSH_ELF_INC_DROP_STOP_USER_SET + stockCode, userId)

							val wodeInfo = new JSONObject()
							wodeInfo.put("stockCode", stockCodeUsual)
							wodeInfo.put("stockName", stockName)
							wodeInfo.put("content", content)
							WodeInfoUtils.message(userId, "涨跌停推送", content, wodeInfo)


							val sqlPush = "insert into push_log(stock_code,user_id,inc_drop_stop,percent_now,sys_create_time) "+ "values('"  + stockCode +"'" + "," + userId + "," + 0  + ","+ stockPercent + ","+    "'" + TimeUtils.getCurrent_time() +"'" + ")"
							val stmtPush = connPush.createStatement()
							stmtPush.executeUpdate(sqlPush)

						})
					}


					if (CollectionUtils.isEmpty( redisStockPushClient.smembers(PushRedisConstants.STOCK_PUSH_ELF_INC_DROP_STOP_USER_SET + stockCode) )) {
						redisStockPushClient.del(PushRedisConstants.STOCK_PUSH_ELF_INC_DROP_STOP_USER_SET + stockCode)
						redisStockPushClient.srem(PushRedisConstants.STOCK_PUSH_ELF_INC_DROP_STOP_STOCK_SET, stockCode)
					}


				})

				RedisStockPushClient.pool.returnResource(redisStockPushClient)
				connPush.close()
			})

		})

		ssc.start()
		ssc.awaitTermination()
	}

	private def getUpStopMessage(stockName: String, stockCode: String, stockPriceClose: Double, stockPercent: String) =  "小沃盯盘提醒您：您的自选股" + stockName + "(" + stockCode + ")于 " + DateUtils.getCurrentFormatTime("yyyy-MM-dd HH:mm:ss") + "上涨" +
	  stockPercent + "%， 已达到今日涨停价 " + stockPriceClose + ", 更多信息请点击查看详情。";

	private def getDownStopMessage(stockName: String, stockCode: String, stockPriceClose: Double, stockPercent: String) =  "小沃盯盘提醒您：您的自选股" + stockName + "(" + stockCode + ")于 " + DateUtils.getCurrentFormatTime("yyyy-MM-dd HH:mm:ss") + "下跌" +
	  stockPercent + "%， 已达到今日跌停价 " + stockPriceClose + ", 更多信息请点击查看详情。";
}
