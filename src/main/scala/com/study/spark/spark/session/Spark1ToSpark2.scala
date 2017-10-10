package com.study.spark.spark.session

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
/**
  * Created by piguanghua on 2017/9/1.
  */
object Spark1ToSpark2 {
	def main(args: Array[String]): Unit = {
		val sparkSession = SparkSession.builder.
		  master("local")
		  .appName("example")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()

		val sparkContext = sparkSession.sparkContext

		val stringArray = Array("a","b","c")
		val rdd = sparkContext.parallelize(stringArray, 1)

		val rowRDD = rdd.map(value => Row.fromSeq(List(value)))

		val schema = StructType(Array(StructField("value",StringType)))

		val stringDS = sparkSession.createDataFrame(rowRDD,schema)

		stringDS.show()
	}
}
