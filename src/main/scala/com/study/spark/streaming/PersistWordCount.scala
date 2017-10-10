package com.study.spark.streaming

import com.study.spark.streaming.mysql.ConnectionPool
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by piguanghua on 2017/9/7.
  */
object PersistWordCount {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		  .setAppName("DirectKafkaWordCount")
		  .setMaster("local[2]")
		val ssc = new StreamingContext(conf, Seconds(1))

		val lines = ssc.socketTextStream("192.168.152.165", 9999)
		val words = lines.flatMap(_.split(" "))
		val pairs = words.map(word=>(word,1))
		val wordCount = pairs.reduceByKey(_ + _)

		Thread.sleep(5000)
		wordCount.print()


		/*wordCount.foreachRDD(rdd=>{
			rdd.foreachPartition(iterator=>{
				val conn = ConnectionPool.getJdbcConn
				iterator.foreach(element=>{
					val sql = "insert into wordcount(word,count) "+ "values('" + element._1 + "'," + element._2 + ")"
					val stmt = conn.createStatement()
					stmt.executeUpdate(sql)
				})
			})
		})*/

		ssc.start()
		ssc.awaitTermination()
	}
}
