package com.study.spark.streaming


import com.alibaba.fastjson.JSONObject
import com.study.spark.config.{PushRedisConstants, StockRedisConstants}
import com.study.spark.pool.{RedisStockInfoClient, RedisStockPushClient}
import com.study.spark.streaming.mysql.{MDBManager, SparkPushConnectionPool}
import com.study.spark.utils._
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.stockemotion.common.utils.{DateUtils, JsonUtils, ObjectUtils}
import com.study.spark.broadcast.StockInfoSink
import com.study.spark.streaming.StockPercentCalculate.log
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.collections.CollectionUtils

import scala.collection.JavaConversions._


/**
  * Created by piguanghua on 2017/9/8.
  */
object StockPriceCalculate {

	val log = org.apache.log4j.LogManager.getLogger("StockPriceCalculate")


	def main(args: Array[String]): Unit = {

		val appName = "StockPriceCalculate"
		val bootStrapServer:String = "192.168.1.226:9092,192.168.1.161:9092,192.168.1.227:9092"
		val zkServerIp:String = "spark1:2181,spark2:2181,spark3:2181"
		val zkAddress:String = "/sparkStreaming/priceCalculate"
		val topics:String = "stockPrice"
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
					//	log.warn("读取的数据："+msg)

						val sqlData = "insert into push_data_receive_log(content,sys_create_date) "+ "values('"  + msg +"'" + "," +  "'"+  TimeUtils.getCurrent_time() +"'" + ")"
						val stmtPush = connPush.createStatement()
						stmtPush.executeUpdate(sqlData)

						val data = JsonUtils.TO_JSONArray(msg._2)

						for(ele <- data){
							val jsonObject =JsonUtils.TO_JSONObject(( ele.toString))
							val stockPrice = jsonObject.get("stockPriceClose").toString().toDouble
							val userPrice = jsonObject.get("userPriceSetting") .toString().toDouble
							val state = jsonObject.get("state").toString.toByte
							val stockCode = jsonObject.get("stockCode").toString
							val userId = jsonObject.get("userId").toString
							val stockName = broadcastVal.value.get(jsonObject.get("stockCode").toString).get
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


									//PushUtils.sendElfPushMessage(stockCodeUsual , stockName, content, redisStockPushClient.get(PushRedisConstants.STOCK_PUSH_USER_CLIENTID + userId), message, deviceType)

									val jsonData = new JSONObject()
									jsonData.put("stockCode", stockCodeUsual)
									jsonData.put("stockName", stockName)
									jsonData.put("content", content)
									//WodeInfoUtils.message(userId, "下跌推送", content, jsonData)



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


									//PushUtils.sendElfPushMessage(stockCodeUsual , stockName, content, redisStockPushClient.get(PushRedisConstants.STOCK_PUSH_USER_CLIENTID + userId), message, deviceType)

									val jsonData = new JSONObject()
									jsonData.put("stockCode", stockCodeUsual)
									jsonData.put("stockName", stockName)
									jsonData.put("content", content)
									//WodeInfoUtils.message(userId, "上涨推送", content, jsonData)

									val sqlPush = "insert into push_log(stock_code,user_id,inc_price,price_now,sys_create_time) "+ "values('"  + stockCode +"'" + "," + userId + "," + userPrice  + ","+ stockPrice + ","+    "'" + TimeUtils.getCurrent_time() +"'" + ")"
									val stmtPush = connPush.createStatement()
									stmtPush.executeUpdate(sqlPush)

								}
							}
						}


						//process(msg)  //处理每条数据

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







	/*	// Create context with 2 second batch interval
		val sparkConf = new SparkConf().setAppName("StockPriceCalculate")
		  .setMaster("spark://spark1:7077")

		//.setMaster("local[2]")
		val ssc = new StreamingContext(sparkConf, Seconds(1))
		val paras = Array("192.168.1.226:9092,192.168.1.161:9092,192.168.1.227:9092", "stockPrice")*/


		/*val stockInfoMap = scala.collection.mutable.Map[String, String]()
		val redisStockInfo = RedisStockInfoClient.getResource()

		//缓存stock数据
		//需要checkpoint

		val stockCodeString = redisStockInfo.lrange(StockRedisConstants.STOCK_ALL_PAN_CODE, 0 ,-1);
		for(jsonString <-  stockCodeString){
			import com.stockemotion.common.utils.JsonUtils
			val jsonObject = JsonUtils.TO_JSONObject(jsonString)
			val stockCode = ObjectUtils.toString(jsonObject.get("stock_code"))
			val stockName = ObjectUtils.toString(jsonObject.get("stock_name"))
			stockInfoMap += (stockCode->stockName)
		}*/

		//大盘
	//	val stockPanCode = redisStockInfo.hgetAll(StockRedisConstants.STOCK_ALL_PAN_CODE)
		import com.alibaba.fastjson.JSONObject
		import com.stockemotion.common.utils.JsonUtils

	/*	for (entry <- stockPanCode.entrySet()) {
			val jsonObject = JsonUtils.TO_JSONObject(entry.getValue())
			val stockCode = ObjectUtils.toString(jsonObject.get("stock_code"))
			val stockName = ObjectUtils.toString(jsonObject.get("stock_name"))
			stockInfoMap.put(stockCode, stockName)
		}*/

		//RedisStockInfoClient.releaseResource(redisStockInfo)

	/*	val Array(brokers, topics) = paras
		val topicsSet = topics.split(",").toSet
		val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
		val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
			ssc, kafkaParams, topicsSet)

		val broadcastVal = StockInfoSink.broadcastStockInfo(ssc.sparkContext)

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

						val stockName = broadcastVal.value.get(jsonObject.get("stockCode").toString).get
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
		ssc.awaitTermination()*/

	}

	private def getPriceDownContent(stockName: String, stockCode: String, stockPriceClose: Double, userPriceSetting: Double) = "小沃盯盘提醒您：您的自选股 " + stockName + "(" + stockCode + ")" + DateUtils.getCurrentFormatTime("yyyy-MM-dd HH:mm:ss") + "下跌到" + StringUtils.formatDouble(stockPriceClose) + " 元, 超过您设置的 " + userPriceSetting + "元，更多信息请点击查看详情"
	private def getPriceUpContent(stockName: String, stockCode: String, stockPriceClose: Double, userPriceSetting: Double) = "小沃盯盘提醒您：您的自选股 " + stockName + "(" + stockCode + ")" + DateUtils.getCurrentFormatTime("yyyy-MM-dd HH:mm:ss") + "上涨到" + StringUtils.formatDouble(stockPriceClose) + " 元, 超过您设置的 " + userPriceSetting + "元，更多信息请点击查看详情"

}
