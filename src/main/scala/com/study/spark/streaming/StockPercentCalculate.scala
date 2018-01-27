package com.study.spark.streaming


import com.alibaba.fastjson.JSONObject
import com.study.spark.config.PushRedisConstants
import com.study.spark.pool.RedisStockPushClient
import com.study.spark.streaming.mysql.MDBManager
import com.study.spark.utils._
import com.stockemotion.common.utils.{JsonUtils, ObjectUtils}
import com.study.spark.broadcast.StockInfoSink
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.collections.CollectionUtils

import scala.collection.JavaConversions._

/**
  * Created by piguanghua on 2017/9/8.
  */
object StockPercentCalculate {


	val log = org.apache.log4j.LogManager.getLogger("StockPercentCalculate")


	def main(args: Array[String]): Unit = {


		val appName = "StockIncDownStopCalculate"
		val bootStrapServer:String = "192.168.1.226:9092,192.168.1.161:9092,192.168.1.227:9092"
		val zkServerIp:String = "192.168.1.213:2181,192.168.1.226:2181,192.168.1.161:2181"
		val zkAddress:String = "/sparkStreaming/percentCalculate"
		val topics:String = "stockPercent"
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
						log.warn("读取的数据："+msg)


						val data = JsonUtils.TO_JSONArray(msg._2)

					/*	for(ele <- data){
							val jsonObject =JsonUtils.TO_JSONObject(( ele.toString))

							val stockPercent = jsonObject.get("stockPercent").toString().toDouble
							val userPercent = jsonObject.get("userPercentSetting") .toString().toDouble
							val state = jsonObject.get("state").toString.toByte

							val stockCode = jsonObject.get("stockCode").toString
							val userId = jsonObject.get("userId").toString

							val stockName = broadcastVal.value.get(jsonObject.get("stockCode").toString).get
							val stockCodeUsual = stockCode.substring(0, stockCode.lastIndexOf("."))



							if(state == 0){  //down
								if(stockPercent <= userPercent){

									redisStockPushClient.srem(PushRedisConstants.STOCK_PUSH_ELF_PERCENTAGE_DOWN_USER_SET + stockCode, userId)
									redisStockPushClient.del(PushRedisConstants.STOCK_PUSH_USER_PERCENTAGE_DOWN + userId + ":" + stockCode)

									if (CollectionUtils.isEmpty(redisStockPushClient.smembers(PushRedisConstants.STOCK_PUSH_ELF_PERCENTAGE_DOWN_USER_SET + stockCode))) {
										redisStockPushClient.del(PushRedisConstants.STOCK_PUSH_ELF_PERCENTAGE_DOWN_USER_SET + stockCode)
										redisStockPushClient.srem(PushRedisConstants.STOCK_PUSH_ELF_PERCENTAGE_DOWN_STOCK_SET, stockCode)
									}


									val content = StockPercentCalculate.getPercentDownContent(stockName, stockCodeUsual, stockPercent, userPercent)

									val deviceType = ObjectUtils.toInteger(redisStockPushClient.get(PushRedisConstants.STOCK_PUSH_USER_DEVICETYPE + userId)).byteValue


									val message = f"下跌$stockPercent 到达你设置的$userPercent%.2f "

									try{
										PushUtils.sendElfPushMessage(stockCodeUsual, stockName, content, redisStockPushClient.get(PushRedisConstants.STOCK_PUSH_USER_CLIENTID + userId), message, deviceType)
									}catch {
										case e:Exception=> println("-------------------------"+ userId)
									}

									val jsonData = new JSONObject()
									jsonData.put("stockCode", stockCodeUsual)
									jsonData.put("stockName", stockName)
									jsonData.put("content", content)
									WodeInfoUtils.message(userId, "跌幅推送", content, jsonData)


									val sqlPush = "insert into push_log(stock_code,user_id,drop_percent,percent_now,sys_create_time) "+ "values('"  + stockCode +"'" + "," + userId + "," + userPercent  + ","+ stockPercent + ","+    "'" + TimeUtils.getCurrent_time() +"'" + ")"
									val stmtPush = connPush.createStatement()
									stmtPush.executeUpdate(sqlPush)

								}
							}else{
								if(stockPercent >= userPercent){

									val redisStockPushClient = RedisStockPushClient.pool.getResource
									redisStockPushClient.srem(PushRedisConstants.STOCK_PUSH_ELF_PERCENTAGE_UP_USER_SET + stockCode, userId)
									redisStockPushClient.del(PushRedisConstants.STOCK_PUSH_USER_PERCENTAGE_UP + userId + ":" + stockCode)

									if (CollectionUtils.isEmpty(redisStockPushClient.smembers(PushRedisConstants.STOCK_PUSH_ELF_PERCENTAGE_UP_USER_SET + stockCode))) {
										redisStockPushClient.del(PushRedisConstants.STOCK_PUSH_ELF_PERCENTAGE_UP_USER_SET + stockCode)
										redisStockPushClient.srem(PushRedisConstants.STOCK_PUSH_ELF_PERCENTAGE_UP_STOCK_SET, stockCode)
									}


									val content = StockPercentCalculate.getPercentUpContent(stockName,stockCodeUsual , stockPercent, userPercent)
									val deviceType = ObjectUtils.toInteger(redisStockPushClient.get(PushRedisConstants.STOCK_PUSH_USER_DEVICETYPE + userId)).byteValue


									val message = f"上涨$stockPercent 到达你设置的$userPercent%.2f "


									PushUtils.sendElfPushMessage(stockCodeUsual, stockName, content, redisStockPushClient.get(PushRedisConstants.STOCK_PUSH_USER_CLIENTID + userId), message, deviceType)

									val jsonData = new JSONObject()
									jsonData.put("stockCode", stockCodeUsual)
									jsonData.put("stockName", stockName)
									jsonData.put("content", content)
									WodeInfoUtils.message(userId, "涨幅推送", content, jsonData)

									val sqlPush = "insert into push_log(stock_code,user_id,inc_percent,percent_now,sys_create_time) "+ "values('"  + stockCode +"'" + "," + userId + "," + userPercent  + ","+  stockPercent + ","+    "'" + TimeUtils.getCurrent_time() +"'" + ")"
									val stmtPush = connPush.createStatement()
									stmtPush.executeUpdate(sqlPush)





								}
							}
						}*/




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









		/*// Create context with 2 second batch interval
		val sparkConf = new SparkConf().setAppName("StockPercentCalculate").setMaster("spark://spark1:7077")//.setMaster("local[2]")
		val ssc = new StreamingContext(sparkConf, Seconds(3))
	//	val paras = Array("spark1:9092,spark2:9092,spark3:9092", "stockPercent")
		val paras = Array("192.168.1.226:9092,192.168.1.161:9092,192.168.1.227:9092", "stockPercent")


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

		//大盘
	//	val stockPanCode = redisStockInfo.lrange(StockRedisConstants.STOCK_ALL_PAN_CODE, 0 ,-1)
		import com.alibaba.fastjson.JSONObject
		import com.stockemotion.common.utils.JsonUtils

		/*for (entry <- stockPanCode.entrySet()) {
			val jsonObject = JsonUtils.TO_JSONObject(entry.getValue())
			val stockCode = ObjectUtils.toString(jsonObject.get("stock_code"))
			val stockName = ObjectUtils.toString(jsonObject.get("stock_name"))
			stockInfoMap.put(stockCode, stockName)
		}*/
		RedisStockInfoClient.releaseResource(redisStockInfo)


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

		event.foreachRDD(jsonArray =>{

			jsonArray.foreachPartition(iterator=>{

			//val jsonArray = new JSONArray()
			val redisStockPushClient = RedisStockPushClient.pool.getResource()
				val connPush = MDBManager.getMDBManager.getConnection


				iterator.foreach(iteratorElement=>{

					for(i <- 0 to iteratorElement.size()-1 ){
						val jsonObject =JsonUtils.TO_JSONObject(( iteratorElement.get(i).toString))

						val stockPercent = jsonObject.get("stockPercent").toString().toDouble
						val userPercent = jsonObject.get("userPercentSetting") .toString().toDouble
						val state = jsonObject.get("state").toString.toByte

						val stockCode = jsonObject.get("stockCode").toString
						val userId = jsonObject.get("userId").toString

						val stockName = stockInfoMap.get(jsonObject.get("stockCode").toString).get
						val stockCodeUsual = stockCode.substring(0, stockCode.lastIndexOf("."))

						if(state == 0){  //down
							if(stockPercent <= userPercent){

								redisStockPushClient.srem(PushRedisConstants.STOCK_PUSH_ELF_PERCENTAGE_DOWN_USER_SET + stockCode, userId)
								redisStockPushClient.del(PushRedisConstants.STOCK_PUSH_USER_PERCENTAGE_DOWN + userId + ":" + stockCode)

								if (CollectionUtils.isEmpty(redisStockPushClient.smembers(PushRedisConstants.STOCK_PUSH_ELF_PERCENTAGE_DOWN_USER_SET + stockCode))) {
									redisStockPushClient.del(PushRedisConstants.STOCK_PUSH_ELF_PERCENTAGE_DOWN_USER_SET + stockCode)
									redisStockPushClient.srem(PushRedisConstants.STOCK_PUSH_ELF_PERCENTAGE_DOWN_STOCK_SET, stockCode)
								}


								val content = StockPercentCalculate.getPercentDownContent(stockName, stockCodeUsual, stockPercent, userPercent)

								val deviceType = ObjectUtils.toInteger(redisStockPushClient.get(PushRedisConstants.STOCK_PUSH_USER_DEVICETYPE + userId)).byteValue


								val message = f"下跌$stockPercent 到达你设置的$userPercent%.2f "

								try{
								PushUtils.sendElfPushMessage(stockCodeUsual, stockName, content, redisStockPushClient.get(PushRedisConstants.STOCK_PUSH_USER_CLIENTID + userId), message, deviceType)
								}catch {
									case e:Exception=> println("-------------------------"+ userId)
								}

								val jsonData = new JSONObject()
								jsonData.put("stockCode", stockCodeUsual)
								jsonData.put("stockName", stockName)
								jsonData.put("content", content)
								WodeInfoUtils.message(userId, "跌幅推送", content, jsonData)


								val sqlPush = "insert into push_log(stock_code,user_id,drop_percent,percent_now,sys_create_time) "+ "values('"  + jsonObject.get("stockCode") +"'" + "," + userId + "," + userPercent  + ","+ stockPercent + ","+    "'" + TimeUtils.getCurrent_time() +"'" + ")"
								val stmtPush = connPush.createStatement()
								stmtPush.executeUpdate(sqlPush)

							}
						}else{
							if(stockPercent >= userPercent){

								val redisStockPushClient = RedisStockPushClient.pool.getResource
								redisStockPushClient.srem(PushRedisConstants.STOCK_PUSH_ELF_PERCENTAGE_UP_USER_SET + stockCode, userId)
								redisStockPushClient.del(PushRedisConstants.STOCK_PUSH_USER_PERCENTAGE_UP + userId + ":" + stockCode)

								if (CollectionUtils.isEmpty(redisStockPushClient.smembers(PushRedisConstants.STOCK_PUSH_ELF_PERCENTAGE_UP_USER_SET + stockCode))) {
									redisStockPushClient.del(PushRedisConstants.STOCK_PUSH_ELF_PERCENTAGE_UP_USER_SET + stockCode)
									redisStockPushClient.srem(PushRedisConstants.STOCK_PUSH_ELF_PERCENTAGE_UP_STOCK_SET, stockCode)
								}


								val content = StockPercentCalculate.getPercentUpContent(stockName,stockCodeUsual , stockPercent, userPercent)
								val deviceType = ObjectUtils.toInteger(redisStockPushClient.get(PushRedisConstants.STOCK_PUSH_USER_DEVICETYPE + userId)).byteValue


								val message = f"上涨$stockPercent 到达你设置的$userPercent%.2f "


								PushUtils.sendElfPushMessage(stockCodeUsual, stockName, content, redisStockPushClient.get(PushRedisConstants.STOCK_PUSH_USER_CLIENTID + userId), message, deviceType)

								val jsonData = new JSONObject()
								jsonData.put("stockCode", stockCodeUsual)
								jsonData.put("stockName", stockName)
								jsonData.put("content", content)
								WodeInfoUtils.message(userId, "涨幅推送", content, jsonData)

								val sqlPush = "insert into push_log(stock_code,user_id,inc_percent,percent_now,sys_create_time) "+ "values('"  + jsonObject.get("stockCode") +"'" + "," + userId + "," + userPercent  + ","+  stockPercent + ","+    "'" + TimeUtils.getCurrent_time() +"'" + ")"
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

	import com.stockemotion.common.utils.DateUtils

	private def getPercentUpContent(stockName: String, stockCode: String, stockPercent: Double, stockPercentSetting: Double) = "小沃盯盘提醒您：您的自选股 " + stockName + "(" + stockCode + ")" + DateUtils.getCurrentFormatTime("yyyy-MM-dd HH:mm:ss") + "涨到" + StringUtils.formatDouble(stockPercent) + " %, 超过您设置的 " + stockPercentSetting + "%，更多信息请点击查看详情"
	private def getPercentDownContent(stockName: String, stockCode: String, stockPercent: Double, stockPercentSetting: Double) = "小沃盯盘提醒您：您的自选股 " + stockName + "(" + stockCode + ")" + DateUtils.getCurrentFormatTime("yyyy-MM-dd HH:mm:ss") + "跌到" + StringUtils.formatDouble(stockPercent) + " %, 超过您设置的 " + stockPercentSetting + "%，更多信息请点击查看详情"

}
