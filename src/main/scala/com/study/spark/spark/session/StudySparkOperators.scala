package com.study.spark.spark.session

import org.apache.spark.sql.SparkSession


/**
  * Created by piguanghua on 2017/10/16.
  */
object StudySparkOperators {
	def main(args: Array[String]): Unit = {
		val sparkSession = SparkSession
		  .builder()
		  .appName("StudySparkOperators")
		  .master("local")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()

		val sparkContext = sparkSession.sparkContext
		val a = sparkContext.parallelize(List("dog", "tiger", "lion", "cat", "panther", " eagle"), 2)
		val b = a.map(x => (x.length, x))
		b.foreach(println(_))
		val c = b.mapValues("x" + _ + "x").collect
		println("-----------------------")
		c.foreach(println(_))



	}
}
