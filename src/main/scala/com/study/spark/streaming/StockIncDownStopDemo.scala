package com.study.spark.streaming

import com.alibaba.fastjson.JSONObject
import com.stockemotion.common.utils.{JsonUtils, ObjectUtils}
import com.study.spark.config.PushRedisConstants
import com.study.spark.pool.RedisStockPushClient
import com.study.spark.streaming.StockIncDownStopCalculate.{getDownStopMessage, getUpStopMessage}
import com.study.spark.streaming.mysql.{MDBManager, SparkPushConnectionPool}
import com.study.spark.utils.{PushUtils, StringUtils, TimeUtils, WodeInfoUtils}
import kafka.serializer.StringDecoder
import org.apache.commons.collections.CollectionUtils

import scala.collection.JavaConversions._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by piguanghua on 2017/10/11.
  */
object StockIncDownStopDemo {
	def main(args: Array[String]): Unit = {
		// Create context with 2 second batch interval
		val sparkConf = new SparkConf().setAppName("StockIncDownStopCalculate").setMaster("local[2]")
		val ssc = new StreamingContext(sparkConf, Seconds(1))
			val paras = Array("spark1:9092,spark2:9092,spark3:9092", "spark")
		val Array(brokers, topics) = paras
		val topicsSet = topics.split(",").toSet
		val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
		val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
			ssc, kafkaParams, topicsSet)

		//收集数据
		val event=messages.flatMap(line => {
			val data = JsonUtils.TO_JSONObject(line._2)
			Some(data)
		})




		event.foreachRDD(jsonArray =>{

			jsonArray.foreachPartition(iterator=>{


				val redisStockPushClient = RedisStockPushClient.pool.getResource()
				val connPush = MDBManager.getMDBManager.getConnection

				iterator.foreach(iteratorElement=>{



					//获取redis push pool
					//解析股票数据
					println("iteratorElement:"+iteratorElement)

					val stockPriceClose = iteratorElement.get("stockPriceClose").toString.toDouble
					val stockPriceHigh = iteratorElement.get("stockPriceHigh").toString.toDouble
					val stockPriceLow = iteratorElement.get("stockPriceLow").toString.toDouble
					val stockPriceYesterday = iteratorElement.get("stockPriceYesterday").toString.toDouble
					val stockCode = iteratorElement.get("stockCode").toString
					val stockName = "天机"


					val stockCodeUsual = stockCode.substring(0, stockCode.lastIndexOf("."))
					val stockPercent = StringUtils.formatDouble( (stockPriceClose - stockPriceYesterday) / stockPriceYesterday )

/*					val connPush= SparkPushConnectionPool.getJdbcConn
					val sqlPush = "insert into kafka_data_log(stock_code,stock_price,stock_price_high,stock_price_low,sys_create_time) "+ "values('"  + stockCode +"'" + "," + stockPriceClose + "," + stockPriceHigh  + ","+ stockPriceLow + ","+    "'" + TimeUtils.getCurrent_time() +"'" + ")"
					val stmtPush = connPush.createStatement()
					stmtPush.executeUpdate(sqlPush)
					SparkPushConnectionPool.releaseConn(connPush)*/

					val sqlPush = "insert into kafka_data_log(stock_code,stock_price,stock_price_high,stock_price_low,sys_create_time) "+ "values('"  + stockCode +"'" + "," + stockPriceClose + "," + stockPriceHigh  + ","+ stockPriceLow + ","+    "'" + TimeUtils.getCurrent_time() +"'" + ")"
					val stmtPush = connPush.createStatement()
					stmtPush.executeUpdate(sqlPush)

					if(stockPriceClose >= stockPriceHigh ){ //涨停

						val userSet = redisStockPushClient.smembers(PushRedisConstants.STOCK_PUSH_ELF_INC_DROP_STOP_USER_SET  + stockCode )

						userSet.foreach(userId=>{

							redisStockPushClient.srem(PushRedisConstants.STOCK_PUSH_ELF_INC_DROP_STOP_USER_SET + stockCode, userId)

							val connPush= SparkPushConnectionPool.getJdbcConn
							val sqlPush = "insert into push_log(stock_code,user_id,inc_drop_stop,percent_now,sys_create_time) "+ "values('"  + stockCode +"'" + "," + userId + "," + 1  + ","+ stockPercent + ","+    "'" + TimeUtils.getCurrent_time() +"'" + ")"
							val stmtPush = connPush.createStatement()
							stmtPush.execute(sqlPush)

						})
					}
					if(stockPriceClose <= stockPriceLow ){

						val userSet = redisStockPushClient.smembers(PushRedisConstants.STOCK_PUSH_ELF_INC_DROP_STOP_USER_SET   + stockCode)

						userSet.foreach(userId=>{

							redisStockPushClient.srem(PushRedisConstants.STOCK_PUSH_ELF_INC_DROP_STOP_USER_SET + stockCode, userId)


							val sqlPush = "insert into push_log(stock_code,user_id,inc_drop_stop,percent_now,sys_create_time) "+ "values('"  + stockCode +"'" + "," + userId + "," + 0  + ","+ stockPercent + ","+    "'" + TimeUtils.getCurrent_time() +"'" + ")"
							val stmtPush = connPush.createStatement()
							stmtPush.execute(sqlPush)
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
}
