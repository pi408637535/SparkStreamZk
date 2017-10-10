package com.study.spark.streaming

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.stockemotion.common.utils.JsonUtils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.util.parsing.json.JSONObject

/**
  * Created by piguanghua on 2017/9/8.
  */
object StockWdCalculate {
	def main(args: Array[String]): Unit = {
		// Create context with 2 second batch interval
		val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")//.setMaster("local[2]")
		val ssc = new StreamingContext(sparkConf, Seconds(60))
		val paras = Array("192.168.152.137:9092,192.168.152.160:9092,192.168.152.163:9092", "stockWd")
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

		val jsonArray = new JSONArray()

		event.reduceByWindow(
			(wdOld,wdNew)=>{
				val wdOldMap = Map
				for(i <- 1 to wdOld.size()){
					val jsonOldElement = wdOld.get(i)
					jsonOldElement
				}
				jsonArray
			},
			Seconds(60),
			Seconds(10)
		)

		event.foreachRDD(jsonArray =>{

			jsonArray.foreachPartition(iterator=>{

				val jsonArray = new JSONArray()

				iterator.foreach(iteratorElement=>{
					for(i <- 1 to iteratorElement.size() - 1 ){
						 var jsonObject =JsonUtils.TO_JSONObject(( iteratorElement.get(i).toString))

						println(jsonObject.get("stockCode") + "-----"+ jsonObject.get("wd"))
					}
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
