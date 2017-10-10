package com.study.spark.streaming

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/9/8.
  */
object Top3HotProduct {
	def main(args: Array[String]): Unit = {

		val host = "192.168.152.137"
		val port = "9999"
		val windowSize = 120
		val slideSize = 80


		val spark = SparkSession
		  .builder
		  .appName("StructuredNetworkWordCount")
		  .master("local")
		  .config("spark.sql.warehouse.dir", "some-value")
		  .getOrCreate()

		import spark.implicits._

		// Create DataFrame representing the stream of input lines from connection to host:port
		val lines = spark.readStream
		  .format("socket")
		  .option("host", host)
		  .option("port", port)
		  //  .option("includeTimestamp", true)
		  .load()

		val words = lines.as[String].flatMap(_.split(" "))
		println(words)
		val wordCounts = words.groupBy("value").count()
		println(wordCounts)

		val query = wordCounts.writeStream
		  .outputMode("complete")
		  .format("console")
		  .start()

		query.awaitTermination()
	}
}