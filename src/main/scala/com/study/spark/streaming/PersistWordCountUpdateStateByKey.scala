package com.study.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by piguanghua on 2017/9/7.
  */
object PersistWordCountUpdateStateByKey {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[2]")
		val ssc = new StreamingContext(conf, Seconds(1))

		val searchLogsDStream = ssc.socketTextStream("192.168.152.137", 9999)
		val words = searchLogsDStream.map {
			_.split(" ")
		}
		val pairs = words.map(word => (word, 1))
		val wordCounts = pairs.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
			val currValue = values.sum
			Some(currValue + state.getOrElse(0))

			/*var newValue= state.getOrElse(0)
		for(value <- values){
			newValue += value
		}
		Option(newValue)*/
		})

		wordCounts.foreachRDD(rdd => {
			rdd.foreachPartition(iterator => {

				iterator.foreach(println(_))

			})
		})

		ssc.start()
		ssc.awaitTermination()
	}
}
