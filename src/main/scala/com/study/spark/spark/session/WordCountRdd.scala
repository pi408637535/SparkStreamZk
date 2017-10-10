package com.study.spark.spark.session

import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/9/5.
  */
object WordCountRdd {
	def main(args: Array[String]): Unit = {
		val sparkSession = SparkSession
		  .builder
		  .appName("example")
	//	  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
	//	  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()

		val file = sparkSession.read.text("hdfs://spark1:9000/word/spark.txt").rdd
		val mapped = file.map(s => s.length).cache()
		for (iter <- 1 to 10) {
			val start = System.currentTimeMillis()
			for (x <- mapped) { x + 2 }
			val end = System.currentTimeMillis()
			println("Iteration " + iter + " took " + (end-start) + " ms")
		}
		sparkSession.stop()
	}
}
