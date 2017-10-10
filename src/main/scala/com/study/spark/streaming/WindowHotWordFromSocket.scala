package com.study.spark.streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
/**
  * Created by piguanghua on 2017/9/7.
  */
object WindowHotWordFromSocket {
	def main(args: Array[String]): Unit = {
		// Create context with 2 second batch interval
		val conf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[2]")
		val ssc = new StreamingContext(conf, Seconds(1))

		val searchLogsDStream = ssc.socketTextStream("192.168.152.137", 9999)
		val searchWordsDStream = searchLogsDStream.map { _.split(" ")(0) }
		val searchWordPairsDStream = searchWordsDStream.map { searchWord => (searchWord, 1) }
		val searchWordCountsDSteram = searchWordPairsDStream.reduceByKeyAndWindow(
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
