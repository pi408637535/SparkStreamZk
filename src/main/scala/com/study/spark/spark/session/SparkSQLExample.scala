package com.study.spark.spark.session

import com.study.spark.constant.Constants
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
case class Person(name: String, age: Long)

/**
  * Created by piguanghua on 2017/9/1.
  */
object SparkSQLExample {
	def main(args: Array[String]): Unit = {
		val sparkSession = SparkSession
		  .builder()
		  .appName(Constants.SPARK_APP_NAME_SESSION)
		  .master("local")
		  .config("spark.sql.warehouse.dir", "some-value")
		  .getOrCreate()

	// runBasicDataFrameExample(sparkSession)
	//	runDatasetCreationExample(sparkSession)
		runInferSchemaExample(sparkSession)
	}

	private def runBasicDataFrameExample(sparkSession: SparkSession): Unit ={
		val df = sparkSession.read.json("D:\\Data\\people.json")
		// For implicit conversions like converting RDDs to DataFrames
		import sparkSession.implicits._
		//	df.show()
		//import spark.implicits._
		//df.printSchema()
		//df.select("name").show()
		//	df.select($"name", $"age" + 1).show()
		//	df.filter($"age" > 21).show()
		// Count people by age
		//df.groupBy("age").count().show()

		// Register the DataFrame as a SQL temporary view
		/*df.createOrReplaceTempView("people")
		val sqlDF = sparkSession.sql("SELECT * FROM people")
		sqlDF.show()*/
	}

	def runDatasetCreationExample(sparkSession: SparkSession): Unit ={
		import sparkSession.implicits._
		val caseClassDS = List(Person("Andy", 32),Person("Andy1", 31)).toDS()
		caseClassDS.show()
		// Encoders for most common types are automatically provided by importing spark.implicits._
	//	val primitiveDS = Seq(1, 2, 3).toDS()

		val peopleDS = sparkSession.read.json("D:\\Data\\people.json").as[Person]
		peopleDS.show(2)
	}

	private def runInferSchemaExample(sparkSession: SparkSession): Unit ={
		import sparkSession.implicits._
		val peopleDF = sparkSession.sparkContext
		  .textFile("D:\\Data\\people.txt")
		  .map(_.split(","))
		  .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
		  .toDF()
		// Register the DataFrame as a temporary view
		peopleDF.createOrReplaceTempView("people")

		// SQL statements can be run by using the sql methods provided by Spark
		val teenagersDF = sparkSession.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

		// The columns of a row in the result can be accessed by field index
		teenagersDF.map(teenager => "Name: " + teenager(0)).show()
		implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
	//	implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()
		teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect().foreach(println(_))


	}
}
