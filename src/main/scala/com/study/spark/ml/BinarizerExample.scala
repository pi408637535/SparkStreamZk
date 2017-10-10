package com.study.spark.ml

import org.apache.spark.ml.feature.DCT
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
/**
  * Created by piguanghua on 2017/10/5.
  */
object BinarizerExample {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
		  .builder
		  .master("local")
		  .appName("TokenizerExample")
		  .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
		  .getOrCreate()


		// Input data: Each row is a bag of words from a sentence or document.
		val documentDF = spark.createDataFrame(Seq(
			"Hi I heard about Spark".split(" "),
			"I wish Java could use case classes".split(" "),
			"Logistic regression models are neat".split(" ")
		).map(Tuple1.apply)).toDF("text")

		// Learn a mapping from words to Vectors.
		val word2Vec:Word2Vec = new org.apache.spark.ml.feature.Word2Vec()
		  .setInputCol("text")
		  .setOutputCol("result")
		  .setVectorSize(3)
		  .setMinCount(0)
		val model = word2Vec.fit(documentDF)

		val result = model.transform(documentDF)
		result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
			println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n") }

		spark.stop()
	}
}
