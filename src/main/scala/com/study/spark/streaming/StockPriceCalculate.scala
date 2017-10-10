package com.study.spark.streaming

import java.util

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.stockemotion.common.utils.JsonUtils
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by piguanghua on 2017/9/8.
  */
object StockPriceCalculate {
	def main(args: Array[String]): Unit = {
		// Create context with 2 second batch interval
		val sparkConf = new SparkConf().setAppName("StockPriceCalculate")// .setMaster("local[2]")
		val ssc = new StreamingContext(sparkConf, Seconds(2))
	//	val paras = Array("192.168.152.137:9092,192.168.152.160:9092,192.168.152.163:9092", "stockPrice")
		val paras = Array("192.168.1.226:9092,192.168.1.161:9092,192.168.1.227:9092", "stockPrice")
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

				val jsonArray = new JSONArray()

				iterator.foreach(iteratorElement=>{
					for(i <- 0 to iteratorElement.size() - 1 ){
						val jsonObject =JsonUtils.TO_JSONObject(( iteratorElement.get(i).toString))

						val stockPrice = jsonObject.get("stockPriceClose").toString().toDouble
						val userPrice = jsonObject.get("userPriceSetting") .toString().toDouble
						val state = jsonObject.get("state").toString.toByte

						if(state == 0){  //down
							if(stockPrice <= userPrice){
								val jsonResult = new JSONObject()
								jsonResult.put("stockCode", jsonObject.get("stockCode"))
								jsonResult.put("userId", jsonObject.get("userId"))
								jsonResult.put("stockPriceClose", stockPrice)
								jsonResult.put("userPriceSetting", userPrice)
								jsonResult.put("state", 0)
								jsonArray.add(jsonResult)
							}
						}else{
							if(stockPrice >= userPrice){
								val jsonResult = new JSONObject()
								jsonResult.put("stockCode", jsonObject.get("stockCode"))
								jsonResult.put("userId", jsonObject.get("userId"))
								jsonResult.put("stockPriceClose", stockPrice)
								jsonResult.put("userPriceSetting", userPrice)
								jsonResult.put("state", 1)
								jsonArray.add(jsonResult)
							}
						}}

					val props = new util.HashMap[String, Object]()
					props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
					props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
						"org.apache.kafka.common.serialization.StringSerializer")
					props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
						"org.apache.kafka.common.serialization.StringSerializer")
					val producer = new KafkaProducer[String,String](props)
					val message=new ProducerRecord[String, String]("stockPriceResult",null,jsonArray.toString)
					producer.send(message)
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
