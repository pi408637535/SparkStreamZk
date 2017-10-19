package com.study.spark.spark.session

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by piguanghua on 2017/10/16.
  */
object StudySparkOperators {
	def main(args: Array[String]): Unit = {

		val sparkConf = new SparkConf().setAppName("StudySparkOperators").setMaster("local[2]")

		// .setMaster("local[2]")
		val ssc = new StreamingContext(sparkConf, Seconds(2))

		ssc.checkpoint("D:\\Data\\checkpoint")

		val lines = ssc.socketTextStream("spark2", 9999)

		val words = lines.flatMap(_.split(" "))

		val pairs = words.map(word=>(word, 1))

		val windowWords = pairs.reduceByKeyAndWindow(_+_,
			(ele1, ele2)=>{
				println("ele1:	" + ele1)
				println("ele2:	"+ele2)
				ele1-ele2
			}, Seconds(6), Seconds(2))

		windowWords.print()
		windowWords.saveAsTextFiles("D:\\Data\\save", "txt")
		ssc.start()
		ssc.awaitTermination()
	}
}
