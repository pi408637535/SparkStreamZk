package com.study.spark.streaming

import java.util

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.stockemotion.common.utils.{DateUtils, JsonUtils, ObjectUtils}
import com.study.spark.broadcast.StockInfoSink
import com.study.spark.pool.{RedisClient, RedisStockInfoClient, RedisStockPushClient}
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConversions._
import com.study.spark.config.PushRedisConstants
import com.study.spark.streaming.StockPercentCalculate.log
import com.study.spark.streaming.mysql.{MDBManager, SparkPushConnectionPool}
import com.study.spark.utils._
import kafka.api.OffsetRequest
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.collections.CollectionUtils

/**
  * Created by piguanghua on 2017/9/13.
  */

object StockIncDownStopCalculate {
	val log = org.apache.log4j.LogManager.getLogger("StockIncDownStopCalculate")


	def main(args: Array[String]): Unit = {

		val appName = "StockIncDownStopCalculate"
		val bootStrapServer:String = "192.168.1.226:9092,192.168.1.161:9092,192.168.1.227:9092"
		val zkServerIp:String = "spark1:2181,spark2:2181,spark3:2181"
		val zkAddress:String = "/sparkStreaming/incDownStop"
		val topics:String = "incDownStopNew"
		val processTime:Long = 3
		val  zkClient= new ZkClient(zkServerIp, 30000, 30000,ZKStringSerializer)

		val sscDStream = SparkDirectStreamingUtils.createStreamingContext(appName,bootStrapServer,zkServerIp,zkAddress,topics,processTime )
		val broadcastVal = StockInfoSink.broadcastStockInfo(sscDStream._1.sparkContext)
		sscDStream._2.foreachRDD( rdd=>{

			if(!rdd.isEmpty()){//只处理有数据的rdd，没有数据的直接跳过


				//迭代分区，里面的代码是运行在executor上面
				rdd.foreachPartition(partitions=>{
					val connPush = MDBManager.getMDBManager.getConnection
					//	val test = redisPoolBroadcastVal.value.jedisPool
					val redisStockPushClient = RedisStockPushClient.pool.getResource()


					//如果没有使用广播变量，连接资源就在这个地方初始化
					//比如数据库连接，hbase，elasticsearch，solr，等等
					//遍历每一个分区里面的消息
					partitions.foreach(msg=>{
						val sqlData = "insert into push_data_receive_log(content,sys_create_date) "+ "values('"  + msg +"'" + "," +  "'"+  TimeUtils.getCurrent_time() +"'" + ")"
						val stmtPush = connPush.createStatement()
						stmtPush.executeUpdate(sqlData)


						val data = JsonUtils.TO_JSONObject(msg._2)
						val stockPriceClose = data.get("stockPriceClose").toString.toDouble
						val stockPriceHigh = data.get("stockPriceHigh").toString.toDouble
						val stockPriceLow = data.get("stockPriceLow").toString.toDouble
						val stockPriceYesterday = data.get("stockPriceYesterday").toString.toDouble
						val stockCode = data.get("stockCode").toString
						val stockName = broadcastVal.value.get(data.get("stockCode").toString).get
						val stockCodeUsual = stockCode.substring(0, stockCode.lastIndexOf("."))
						val stockPercent = StringUtils.formatDouble( (stockPriceClose - stockPriceYesterday) / stockPriceYesterday * 100 )

						/*if(stockPriceClose >= stockPriceHigh ){ //涨停

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
						}*/




					})

					RedisStockPushClient.pool.returnResource(redisStockPushClient)
					connPush.close()
				})



				//更新每个批次的偏移量到zk中，注意这段代码是在driver上执行的
				KafkaOffsetManager.saveOffsets(zkClient,zkAddress,rdd)
			}

		})

		sscDStream._1.start()
		SparkDirectStreamingUtils.daemonHttpServer(5555,sscDStream._1)
		sscDStream._1.awaitTermination()



		/*val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
			ssc, kafkaParams, topicsSet)*/

		/*val stockInfoMap = scala.collection.mutable.Map[String, String]()
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
		val stockInfoBroadcast:Broadcast[scala.collection.mutable.Map[String, String]] = sc.broadcast(stockInfoMap)*/



	//	val broadcastVal = StockInfoSink.broadcastStockInfo(ssc.sparkContext)
	//	val redisPoolBroadcastVal = ssc.sparkContext.broadcast(new RedisClient)

		//收集数据
		/*val event=stream.flatMap(line => {
			val data = JsonUtils.TO_JSONObject(line._2)
			Some(data)
		})*/

		/*event.foreachRDD(jsonArray =>{

			jsonArray.foreachPartition(iterator=>{
				val connPush = MDBManager.getMDBManager.getConnection
			//	val test = redisPoolBroadcastVal.value.jedisPool
				val redisStockPushClient = RedisStockPushClient.pool.getResource()

				iterator.foreach(iteratorElement=>{

					//获取redis push pool
					//解析股票数据
					val stockPriceClose = iteratorElement.get("stockPriceClose").toString.toDouble
					val stockPriceHigh = iteratorElement.get("stockPriceHigh").toString.toDouble
					val stockPriceLow = iteratorElement.get("stockPriceLow").toString.toDouble
					val stockPriceYesterday = iteratorElement.get("stockPriceYesterday").toString.toDouble
					val stockCode = iteratorElement.get("stockCode").toString

					val stockName = broadcastVal.value.get(iteratorElement.get("stockCode").toString).get
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

		})*/

	}

	private def getUpStopMessage(stockName: String, stockCode: String, stockPriceClose: Double, stockPercent: String) =  "小沃盯盘提醒您：您的自选股" + stockName + "(" + stockCode + ")于 " + DateUtils.getCurrentFormatTime("yyyy-MM-dd HH:mm:ss") + "上涨" +
	  stockPercent + "%， 已达到今日涨停价 " + stockPriceClose + ", 更多信息请点击查看详情。";

	private def getDownStopMessage(stockName: String, stockCode: String, stockPriceClose: Double, stockPercent: String) =  "小沃盯盘提醒您：您的自选股" + stockName + "(" + stockCode + ")于 " + DateUtils.getCurrentFormatTime("yyyy-MM-dd HH:mm:ss") + "下跌" +
	  stockPercent + "%， 已达到今日跌停价 " + stockPriceClose + ", 更多信息请点击查看详情。";
}
