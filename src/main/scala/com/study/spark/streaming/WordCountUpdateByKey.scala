package com.study.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by piguanghua on 2017/9/6.
  */
object WordCountUpdateByKey {
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")//.setMaster("local[2]")
	//	sparkConf.set("spark.testing.memory", "21474800000")
		val ssc = new StreamingContext(sparkConf, Seconds(10))
		ssc.checkpoint("hdfs://spark1:9000/wordcount_ckeckpoint")
		val paras = Array("192.168.152.137:9092,192.168.152.160:9092,192.168.152.162:9092","spark")
		val Array(brokers, topics) = paras
		// Create direct kafka stream with brokers and topics
		//val topicsSet = topics.split(",").toSet
		val topicsSet = topics.split(",").toSet
		val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
		val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
			ssc, kafkaParams, topicsSet)

		val lines = messages.map(_._2)
		val words = lines.flatMap(_.split(" "))
		val pairs = words.map(word => (word, 1))
		val wordCounts = pairs.updateStateByKey((values:Seq[Int],state:Option[Int])=>{
			val currValue = values.sum
			Some(currValue+state.getOrElse(0))

			/*var newValue= state.getOrElse(0)
			for(value <- values){
				newValue += value
			}
			Option(newValue)*/
		})
		wordCounts.print()
		// Start the computation
		ssc.start()
		ssc.awaitTermination()
	}
}
