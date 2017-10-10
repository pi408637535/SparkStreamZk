package com.study.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by piguanghua on 2017/9/5.
  */
object DirectKafkaWordCount {
	def main(args: Array[String]): Unit = {
		// Create context with 2 second batch interval
		val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")//.setMaster("local[2]")
		val ssc = new StreamingContext(sparkConf, Seconds(10))
		val paras = Array("192.168.152.163:9092,192.168.152.164:9092,192.168.152.165:9092","test")
		val Array(brokers, topics) = paras
		// Create direct kafka stream with brokers and topics
		val topicsSet = topics.split(",").toSet
		val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
		val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
			ssc, kafkaParams, topicsSet)

		// Get the lines, split them into words, count the words and print
		val lines = messages.map(_._2)
		val words = lines.flatMap(_.split(" "))
		val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
		wordCounts.print()

		// Start the computation
		ssc.start()
		ssc.awaitTermination()
	}
}
