package com.study.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by piguanghua on 2017/9/7.
  */
object WindowHotWord {
	def main(args: Array[String]): Unit = {
		// Create context with 2 second batch interval
		val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[2]")
		val ssc = new StreamingContext(sparkConf, Seconds(10))
		val paras = Array("192.168.152.137:9092,192.168.152.160:9092,192.168.152.162:9092", "test")
		val Array(brokers, topics) = paras
		// Create direct kafka stream with brokers and topics
		val topicsSet = topics.split(",").toSet
		val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
		val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
			ssc, kafkaParams, topicsSet)

		val lines = messages.map(_._2)
		val words = lines.flatMap(_.split(" "))
		val pairs = words.map(word => (word, 1))

		val searchWordCountsDSteram = pairs.reduceByKeyAndWindow(
			(v1: Int, v2: Int) => v1 + v2,
			Seconds(60),
			Seconds(10))

		val finalDStream = searchWordCountsDSteram.transform(searchWordCountsRDD => {
			val countSearchWordsRDD = searchWordCountsRDD.map(tuple => (tuple._2, tuple._1))
			val sortedCountSearchWordsRDD = countSearchWordsRDD.sortByKey(false)
			val sortedSearchWordCountsRDD = sortedCountSearchWordsRDD.map(tuple => (tuple._1, tuple._2))

			val top3SearchWordCounts = sortedSearchWordCountsRDD.take(3)
			for(tuple <- top3SearchWordCounts) {
				println(tuple)
			}

			searchWordCountsRDD
		})


		finalDStream.print()

		ssc.start()
		ssc.awaitTermination()
	}
}
