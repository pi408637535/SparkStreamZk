package com.study.spark.streaming

import java.sql.{Date, Timestamp}
import java.util

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.stockemotion.common.utils.{JsonUtils, ObjectUtils}
import com.study.spark.pool.{KafkaPoolUtils, RedisStockInfoClient, RedisStockPushClient}
import com.study.spark.streaming.mysql.{SparkConnectionPool, SparkPushConnectionPool}
import com.study.spark.utils.StringUtils
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.JavaConversions._

/**
  * Created by piguanghua on 2017/9/8.
  */
object StockPercentCalculate {
	def main(args: Array[String]): Unit = {
		// Create context with 2 second batch interval
		val sparkConf = new SparkConf().setAppName("StockPriceCalculate")//.setMaster("local[2]")
		val ssc = new StreamingContext(sparkConf, Seconds(3))
	//	val paras = Array("spark1:9092,spark2:9092,spark3:9092", "stockPercent")
		val paras = Array("192.168.1.226:9092,192.168.1.161:9092,192.168.1.227:9092", "stockPercent")


		val stockInfoMap = scala.collection.mutable.Map[String, String]()

		//缓存stock数据
		val redisStockInfo = RedisStockInfoClient.pool.getResource
		val stockCodeString = redisStockInfo.lrange("CN:STK:CODELIST:APP:TEST", 0 ,-1);
		for(jsonString <-  stockCodeString){
			import com.stockemotion.common.utils.JsonUtils
			val jsonObject = JsonUtils.TO_JSONObject(jsonString)
			val stockCode = ObjectUtils.toString(jsonObject.get("stock_code"))
			val stockName = ObjectUtils.toString(jsonObject.get("stock_name"))
			stockInfoMap += (stockCode->stockName)
		}

	/*	//大盘
		val stockPanCode = redisStockInfo.hgetAll("CN:STK:INDEXLIST:APP")
		import com.alibaba.fastjson.JSONObject
		import com.stockemotion.common.utils.JsonUtils

		for (jsonString <- stockPanCode) {
			val jsonObject = JsonUtils.TO_JSONObject(jsonString)
			val stockCode = ObjectUtils.toString(jsonObject.get("stock_code"))
			val stockName = ObjectUtils.toString(jsonObject.get("stock_name"))
			stockInfoMap.put(stockCode, stockName)
		}*/


		val Array(brokers, topics) = paras
		val topicsSet = topics.split(",").toSet
		val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
		val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
			ssc, kafkaParams, topicsSet)

		//收集数据
		val event=messages.flatMap(line => {

			val data = JsonUtils.TO_JSONArray(line._2)


			Some(data)
		})

		event.print()

		event.foreachRDD(jsonArray =>{

			jsonArray.foreachPartition(iterator=>{

			val jsonArray = new JSONArray()

				iterator.foreach(iteratorElement=>{
					for(i <- 0 to iteratorElement.size()-1 ){
						val jsonObject =JsonUtils.TO_JSONObject(( iteratorElement.get(i).toString))

						val stockPercent = jsonObject.get("stockPercent").toString().toDouble
						val userPercent = jsonObject.get("userPercentSetting") .toString().toDouble
						val state = jsonObject.get("state").toString.toByte

						println("data：" +  "stockPercent:" + stockPercent + "---" + "userPercent:" + userPercent)

						if(state == 0){  //down
							if(stockPercent <= userPercent){
								val jsonResult = new JSONObject()
								jsonResult.put("stockCode", jsonObject.get("stockCode"))
								jsonResult.put("userId", jsonObject.get("userId"))
								jsonResult.put("stockPercent", stockPercent)
								jsonResult.put("userPercentSetting", userPercent)
								jsonResult.put("state", 0)
								jsonArray.add(jsonResult)

								val conn = SparkConnectionPool.getJdbcConn
								val sql = "insert into stock_state(stock_name,stockPercent,userPercentSetting,state,time) "+ "values('" + jsonObject.get("stockCode") +"'" + "," + stockPercent + ","+ userPercent + ","+  0 +","+   "'" + new Timestamp(new Date(System.currentTimeMillis()).getTime) +"'" + ")"
								val stmt = conn.createStatement()
								//stmt.executeUpdate(sql)

								SparkConnectionPool.releaseConn(conn)
							}
						}else{
							if(stockPercent >= userPercent){
								val jsonResult = new JSONObject()
								jsonResult.put("stockCode", jsonObject.get("stockCode"))
								jsonResult.put("userId", jsonObject.get("userId"))
								jsonResult.put("stockPercent", stockPercent)
								jsonResult.put("userPercentSetting", userPercent)
								jsonResult.put("state", 1)
								// jsonArray.add(jsonResult)

								val stockCode = jsonObject.get("stockCode").toString
								val userId = jsonObject.get("userId").toString


								val stockName = stockInfoMap.get(jsonObject.get("stockCode").toString)
								val stockCodeUsual = jsonObject.get("stockCode").toString.substring(0, jsonObject.get("stockCode").toString.lastIndexOf("."))
								//mysql insert

								import com.stockemotion.common.utils.ObjectUtils
								import org.apache.commons.collections.CollectionUtils

								val redisStockPushClient = RedisStockPushClient.pool.getResource
								redisStockPushClient.srem("STOCK:PUSH:ELF:PERCENTAGE:UP:USER:SET:"+ jsonObject.get("stockCode").toString, jsonObject.get("userId").toString)
								redisStockPushClient.del("STOCK:SPECIAL:USER:ELF:PERCENTAGE:UP:" + jsonObject.get("userId").toString + ":" + jsonObject.get("stockCode").toString)

								if (CollectionUtils.isEmpty(redisStockPushClient.smembers("STOCK:PUSH:ELF:PERCENTAGE:UP:USER:SET:" + jsonObject.get("stockCode").toString))) {
									redisStockPushClient.del("STOCK:PUSH:ELF:PERCENTAGE:UP:USER:SET:" + jsonObject.get("stockCode").toString)
									redisStockPushClient.srem("STOCK:PUSH:ELF:PERCENTAGE:DOWN:STOCK:SET", jsonObject.get("stockCode").toString)
								}


							//	val content = StockPercentCalculate.getPercentUpContent(stockName.get, stockCode, stockPercent, userPercent)
							//	val deviceType = ObjectUtils.toInteger(redisStockPushClient.get("STOCK:PUSH:USER:DEVICETYPE:" + userId)).byteValue

							//	val message = String.format("上涨%1$s%%, 达到您设置的%2$s%%", stockPercent, userPercent)

								//PushUtils.sendElfPushMessage(stockCodeUsual, stockName, content, jedisPushDAO.getJedisReadTemplate.get(STOCK_PUSH_USER_CLIENTID + userId), message, deviceType)


								val connSpark = SparkConnectionPool.getJdbcConn
								val sqlSpark = "insert into stock_state(stock_name,stockPercent,userPercentSetting,state,time) "+ "values('" + jsonObject.get("stockCode") +"'" + "," + stockPercent + ","+ userPercent + ","+  1 +","+   "'" + new Timestamp(new Date(System.currentTimeMillis()).getTime) +"'" + ")"
								val stmtSpark = connSpark.createStatement()
								stmtSpark.executeUpdate(sqlSpark)
								SparkConnectionPool.releaseConn(connSpark)

								val connPush= SparkPushConnectionPool.getJdbcConn
								val sqlPush = "insert into push_log(stock_code,user_id,inc_percent,percent_now,sys_create_time) "+ "values('"  + jsonObject.get("stockCode") +"'" + "," + 11 + "," + stockPercent + ","+ userPercent + ","+    "'" + new Timestamp(new Date(System.currentTimeMillis()).getTime) +"'" + ")"
								val stmtPush = connPush.createStatement()
								stmtPush.executeUpdate(sqlPush)
								SparkPushConnectionPool.releaseConn(connPush)




							}
						}}

					val sender = KafkaPoolUtils.getKafkaSender()
					sender.send(jsonArray.toString, KafkaPoolUtils.getStringEncoder())
					KafkaPoolUtils.returnSender(sender)

				})
			})
			println("************************************************************")
		})


		//	event.print()
		//	event.print(1)
		ssc.start()
		ssc.awaitTermination()

	}

	import com.stockemotion.common.utils.DateUtils

//	private def getPercentUpContent(stockName: String, stockCode: String, stockPercent: Double, stockPercentSetting: Double) = "小沃盯盘提醒您：您的自选股 " + stockName + "(" + stockCode + ")" + DateUtils.getCurrentFormatTime("yyyy-MM-dd HH:mm:ss") + "涨到" + StringUtils.formatDouble(stockPercent) + " %, 超过您设置的 " + stockPercentSetting + "%，更多信息请点击查看详情"
}
