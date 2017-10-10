package com.study.spark.streaming

import java.sql.{Date, Timestamp}
import java.util

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.stockemotion.common.utils.JsonUtils
import com.study.spark.pool.kafka.KafkaPoolUtils
import com.study.spark.streaming.mysql.ConnectionPool
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

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

								val conn = ConnectionPool.getJdbcConn
								val sql = "insert into stock_state(stock_name,stockPercent,userPercentSetting,state,time) "+ "values('" + jsonObject.get("stockCode") +"'" + "," + stockPercent + ","+ userPercent + ","+  1 +","+   "'" + new Timestamp(new Date(System.currentTimeMillis()).getTime) +"'" + ")"
								val stmt = conn.createStatement()
								stmt.executeUpdate(sql)

								ConnectionPool.releaseConn(conn)
							}
						}else{
							if(stockPercent >= userPercent){
								val jsonResult = new JSONObject()
								jsonResult.put("stockCode", jsonObject.get("stockCode"))
								jsonResult.put("userId", jsonObject.get("userId"))
								jsonResult.put("stockPercent", stockPercent)
								jsonResult.put("userPercentSetting", userPercent)
								jsonResult.put("state", 1)
								jsonArray.add(jsonResult)

								val conn = ConnectionPool.getJdbcConn
								val sql = "insert into stock_state(stock_name,stockPercent,userPercentSetting,state,time) "+ "values('" + jsonObject.get("stockCode") +"'" + "," + stockPercent + ","+ userPercent + ","+  1 +","+   "'" + new Timestamp(new Date(System.currentTimeMillis()).getTime) +"'" + ")"
								val stmt = conn.createStatement()
								stmt.executeUpdate(sql)
								ConnectionPool.releaseConn(conn)
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
}
