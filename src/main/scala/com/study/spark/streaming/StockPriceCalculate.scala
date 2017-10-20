package com.study.spark.streaming


import com.study.spark.config.{PushRedisConstants, StockRedisConstants}
import com.study.spark.pool.{RedisStockInfoClient, RedisStockPushClient}
import com.study.spark.streaming.mysql.{MDBManager, SparkPushConnectionPool}
import com.study.spark.utils.{PushUtils, StringUtils, TimeUtils, WodeInfoUtils}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.stockemotion.common.utils.{DateUtils, ObjectUtils}
import org.apache.commons.collections.CollectionUtils

import scala.collection.JavaConversions._

/**
  * Created by piguanghua on 2017/9/8.
  */
object StockPriceCalculate {
	def main(args: Array[String]): Unit = {
		// Create context with 2 second batch interval
		val sparkConf = new SparkConf().setAppName("StockPriceCalculate")//.setMaster("local[2]")
		val ssc = new StreamingContext(sparkConf, Seconds(6))
		val paras = Array("192.168.1.226:9092,192.168.1.161:9092,192.168.1.227:9092", "stockPrice")


		val stockInfoMap = scala.collection.mutable.Map[String, String]()
		val redisStockInfo = RedisStockInfoClient.getResource()

		//缓存stock数据
		//需要checkpoint

		val stockCodeString = redisStockInfo.lrange(StockRedisConstants.STOCK_ALL_CODE, 0 ,-1);
		for(jsonString <-  stockCodeString){
			import com.stockemotion.common.utils.JsonUtils
			val jsonObject = JsonUtils.TO_JSONObject(jsonString)
			val stockCode = ObjectUtils.toString(jsonObject.get("stock_code"))
			val stockName = ObjectUtils.toString(jsonObject.get("stock_name"))
			stockInfoMap += (stockCode->stockName)
		}

		//大盘
		val stockPanCode = redisStockInfo.hgetAll(StockRedisConstants.STOCK_ALL_PAN_CODE)
		import com.alibaba.fastjson.JSONObject
		import com.stockemotion.common.utils.JsonUtils

		for (entry <- stockPanCode.entrySet()) {
			val jsonObject = JsonUtils.TO_JSONObject(entry.getValue())
			val stockCode = ObjectUtils.toString(jsonObject.get("stock_code"))
			val stockName = ObjectUtils.toString(jsonObject.get("stock_name"))
			stockInfoMap.put(stockCode, stockName)
		}

		RedisStockInfoClient.releaseResource(redisStockInfo)

		val Array(brokers, topics) = paras
		val topicsSet = topics.split(",").toSet
		val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
		val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
			ssc, kafkaParams, topicsSet)

		//收集数据
		val event=messages.flatMap(line => {
			val look = line._1
			val data = JsonUtils.TO_JSONArray(line._2)

			Some(data)
		})

		event.foreachRDD(jsonArray =>{

			jsonArray.foreachPartition(iterator=>{

				val redisStockPushClient = RedisStockPushClient.pool.getResource()
				val connPush = MDBManager.getMDBManager.getConnection

				iterator.foreach(iteratorElement=>{
					for(i <- 0 to iteratorElement.size() - 1 ){
						val jsonObject =JsonUtils.TO_JSONObject(( iteratorElement.get(i).toString))

						val stockPrice = jsonObject.get("stockPriceClose").toString().toDouble
						val userPrice = jsonObject.get("userPriceSetting") .toString().toDouble
						val state = jsonObject.get("state").toString.toByte

						val stockCode = jsonObject.get("stockCode").toString
						val userId = jsonObject.get("userId").toString

						val stockName = stockInfoMap.get(jsonObject.get("stockCode").toString).get
						val stockCodeUsual = stockCode.substring(0, stockCode.lastIndexOf("."))

						if(state == 0){  //down
							if(stockPrice <= userPrice){

								val redisStockPushClient = RedisStockPushClient.pool.getResource
								redisStockPushClient.srem(PushRedisConstants.STOCK_PUSH_ELF_PRICE_DOWN_USER_SET + stockCode, userId)
								redisStockPushClient.del(PushRedisConstants.STOCK_PUSH_USER_PRICE_DOWN + userId + ":" + stockCode)

								if (CollectionUtils.isEmpty(redisStockPushClient.smembers(PushRedisConstants.STOCK_PUSH_ELF_PRICE_DOWN_USER_SET + stockCode))) {
									redisStockPushClient.del(PushRedisConstants.STOCK_PUSH_ELF_PRICE_DOWN_USER_SET + stockCode)
									redisStockPushClient.srem(PushRedisConstants.STOCK_PUSH_ELF_PRICE_DOWN_STOCK_SET, stockCode)
								}


								val content = StockPriceCalculate.getPriceDownContent(stockName, stockCodeUsual, stockPrice, userPrice)
								val deviceType = ObjectUtils.toInteger(redisStockPushClient.get(PushRedisConstants.STOCK_PUSH_USER_DEVICETYPE + userId)).byteValue


								val message = f"下跌$stockPrice 到达你设置的$userPrice%.2f "


								PushUtils.sendElfPushMessage(stockCodeUsual , stockName, content, redisStockPushClient.get(PushRedisConstants.STOCK_PUSH_USER_CLIENTID + userId), message, deviceType)

								val jsonData = new JSONObject()
								jsonData.put("stockCode", stockCodeUsual)
								jsonData.put("stockName", stockName)
								jsonData.put("content", content)
								WodeInfoUtils.message(userId, "下跌推送", content, jsonData)



								val sqlPush = "insert into push_log(stock_code,user_id,drop_price,price_now,sys_create_time) "+ "values('"  + stockCode +"'" + "," + userId + "," + userPrice  + ","+ stockPrice + ","+    "'" + TimeUtils.getCurrent_time() +"'" + ")"
								try{
								val stmtPush = connPush.createStatement()
								stmtPush.executeUpdate(sqlPush)
								}catch {
									case ex:Exception=>{
										val stmtPush = connPush.createStatement()
										val sqlPush = "insert into push_error_log(stock_code,user_id,drop_price,sys_create_time) "+ "values(    '"  +   stockCode  +"'" + "," + userId  + "," +  userPrice  + "," + "'" + TimeUtils.getCurrent_time() +"'" + ")"
										stmtPush.executeUpdate(sqlPush)
									}

								}
							}
						}else{
							if(stockPrice >= userPrice){

								val redisStockPushClient = RedisStockPushClient.pool.getResource
								redisStockPushClient.srem(PushRedisConstants.STOCK_PUSH_ELF_PRICE_UP_USER_SET + stockCode, userId)
								redisStockPushClient.del(PushRedisConstants.STOCK_PUSH_USER_PRICE_UP + userId + ":" + stockCode)

								if (CollectionUtils.isEmpty(redisStockPushClient.smembers(PushRedisConstants.STOCK_PUSH_ELF_PRICE_UP_USER_SET + stockCode))) {
									redisStockPushClient.del(PushRedisConstants.STOCK_PUSH_ELF_PRICE_UP_USER_SET + stockCode)
									redisStockPushClient.srem(PushRedisConstants.STOCK_PUSH_ELF_PRICE_UP_STOCK_SET, stockCode)
								}


								val content = StockPriceCalculate.getPriceUpContent(stockName, stockCodeUsual, stockPrice, userPrice)
								val deviceType = ObjectUtils.toInteger(redisStockPushClient.get(PushRedisConstants.STOCK_PUSH_USER_DEVICETYPE + userId)).byteValue


								val message = f"上涨$stockPrice 到达你设置的$userPrice%.2f "


								PushUtils.sendElfPushMessage(stockCodeUsual , stockName, content, redisStockPushClient.get(PushRedisConstants.STOCK_PUSH_USER_CLIENTID + userId), message, deviceType)

								val jsonData = new JSONObject()
								jsonData.put("stockCode", stockCodeUsual)
								jsonData.put("stockName", stockName)
								jsonData.put("content", content)
								WodeInfoUtils.message(userId, "上涨推送", content, jsonData)

								val sqlPush = "insert into push_log(stock_code,user_id,inc_price,price_now,sys_create_time) "+ "values('"  + stockCode +"'" + "," + userId + "," + userPrice  + ","+ stockPrice + ","+    "'" + TimeUtils.getCurrent_time() +"'" + ")"
								val stmtPush = connPush.createStatement()
								stmtPush.executeUpdate(sqlPush)

							}
						}}
				})
				RedisStockPushClient.pool.returnResource(redisStockPushClient)
				connPush.close()
			})
		})


		//	event.print()
		//	event.print(1)
		ssc.start()
		ssc.awaitTermination()

	}

	private def getPriceDownContent(stockName: String, stockCode: String, stockPriceClose: Double, userPriceSetting: Double) = "小沃盯盘提醒您：您的自选股 " + stockName + "(" + stockCode + ")" + DateUtils.getCurrentFormatTime("yyyy-MM-dd HH:mm:ss") + "下跌到" + StringUtils.formatDouble(stockPriceClose) + " 元, 超过您设置的 " + userPriceSetting + "元，更多信息请点击查看详情"
	private def getPriceUpContent(stockName: String, stockCode: String, stockPriceClose: Double, userPriceSetting: Double) = "小沃盯盘提醒您：您的自选股 " + stockName + "(" + stockCode + ")" + DateUtils.getCurrentFormatTime("yyyy-MM-dd HH:mm:ss") + "上涨到" + StringUtils.formatDouble(stockPriceClose) + " 元, 超过您设置的 " + userPriceSetting + "元，更多信息请点击查看详情"

}
