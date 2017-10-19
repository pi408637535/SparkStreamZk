package com.study.spark.streaming

import com.stockemotion.common.utils.{JsonUtils, ObjectUtils}
import com.study.spark.config.{ConfigurationManager, StockRedisConstants}
import com.study.spark.pool.CheckMysqlPool.connPush
import com.study.spark.pool.RedisStockInfoClient
import com.study.spark.streaming.mysql.MDBManager
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import com.study.spark.utils.StockEnumeration._
import com.study.spark.utils.TimeUtils

/**
  * Created by piguanghua on 2017/10/16.
  */
object StockWodeSignalCalculateDemo {
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("StockWodeSignalCalculate").setMaster("local[2]")

		// .setMaster("local[2]")
		val ssc = new StreamingContext(sparkConf, Seconds(6))

	//	ssc.checkpoint("hdfs://spark1:9000/wode/stock/wd")
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




		val kafkaToupleData =
		messages
		  .map(line =>
			  {
				  val data = JsonUtils.TO_JSONObject(line._2)
				   ( ObjectUtils.toString(data.get("stockCode")) ,  ObjectUtils.toInteger(data.get("wodeSignal")).toInt   )
			//	  (line,1)
			  })


		kafkaToupleData.foreachRDD(elementRdd=>{
			elementRdd.foreachPartition(elementPartition=>{

				val connPushGold = MDBManager.getMDBManager.getConnection

				elementPartition.foreach(element=> {
					val sqlPush = "insert into wd_signal_log(stock_code,wd_signal,sys_create_time) "+ "values('"  +   element._1  +"'" + "," + element._2  + "," +   "'" + TimeUtils.getCurrent_time() +"'" + ")"
					val stmtPush = connPushGold.createStatement()
					stmtPush.executeUpdate(sqlPush)
				})
				connPushGold.close()
			})
		})


		val windowData =
			kafkaToupleData.reduceByKeyAndWindow(
			(oldStockWd,newStockWd)=>{   //key值相同，如何过滤
				if(oldStockWd == Sell.id && newStockWd == Buy.id )
					Sell2Buy.id
				else if(oldStockWd == Empty.id && newStockWd == Buy.id )
					Empty2Buy.id
				else if(oldStockWd == Buy.id && newStockWd == Sell.id )
					Buy2Sell.id
				else if(oldStockWd == Hold.id && newStockWd == Sell.id )
					Hold2Sell.id
				else
					newStockWd
			},
			(reduceWd,newStockWd)=>{   //key值相同，如何过滤
				newStockWd
			}
			,Seconds(6),Seconds(6))

		windowData.print()

		windowData.foreachRDD(elementRdd=>{
			elementRdd.foreachPartition(elementPartition=>{

				val connPush = MDBManager.getMDBManager.getConnection


				elementPartition.foreach(element=> {

					element._2 match {
						case 13 =>{
							val sqlPush = "insert into wd_signal(stock_code,wd_signal,wd_state,sys_create_time) "+ "values('"  + element._1 +"'" + "," + 1 + "," + element._2  + "," +   "'" + TimeUtils.getCurrent_time() +"'" + ")"
							val stmtPush = connPush.createStatement()
							stmtPush.executeUpdate(sqlPush)
							println(Sell2Buy.id)
						}
						case 23 =>{
							val sqlPush = "insert into wd_signal(stock_code,wd_signal,wd_state,sys_create_time) "+ "values('"  + element._1 +"'" + "," + 1 + "," + element._2  + "," +   "'" + TimeUtils.getCurrent_time() +"'" + ")"
							val stmtPush = connPush.createStatement()
							stmtPush.executeUpdate(sqlPush)
							println(Empty2Buy.id)

						}
						case 31 =>{
							val sqlPush = "insert into wd_signal(stock_code,wd_signal,wd_state,sys_create_time) "+ "values('"  + element._1 +"'" + "," + 1 + "," + element._2  + "," +   "'" + TimeUtils.getCurrent_time() +"'" + ")"
							val stmtPush = connPush.createStatement()
							stmtPush.executeUpdate(sqlPush)
							println(Buy2Sell.id)
						}
						case 41 =>{
							val sqlPush = "insert into wd_signal(stock_code,wd_signal,wd_state,sys_create_time) "+ "values('"  + element._1 +"'" + "," + 1 + "," + element._2  + "," +   "'" + TimeUtils.getCurrent_time() +"'" + ")"
							val stmtPush = connPush.createStatement()
							stmtPush.executeUpdate(sqlPush)
							println(Hold2Sell.id)
						}
					}
				})
				connPush.close()
			})
		})



		ssc.start()
		ssc.awaitTermination()
	}



}
