package com.study.spark.spark.session

import com.study.spark.constant.Constants
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
/**
  * Created by piguanghua on 2017/9/1.
  */
object UserDefinedUntypedAggregation {
	object MyAverage extends UserDefinedAggregateFunction{
		override def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

		override def bufferSchema: StructType = StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)

		override def dataType: DataType = DoubleType

		override def deterministic: Boolean = true

		override def initialize(buffer: MutableAggregationBuffer): Unit = {
			buffer(0) = 0L
			buffer(1) = 0L
		}

		override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
			if (!input.isNullAt(0)) {
				buffer(0) = buffer.getLong(0) + input.getLong(0)
				buffer(1) = buffer.getLong(1) + 1
			}
		}

		override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
			buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
			buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
		}

		override def evaluate(buffer: Row): Any = buffer.getLong(0).toDouble / buffer.getLong(1)
	}

	def main(args: Array[String]): Unit = {
		val sparkSession = SparkSession
		  .builder()
		  .appName(Constants.SPARK_APP_NAME_SESSION)
		  .master("local")
		  .config("spark.sql.warehouse.dir", "some-value")
		  .getOrCreate()

		// Register the function to access it
		sparkSession.udf.register("myAverage", MyAverage)
		val df = sparkSession.read.json("D:\\Data\\employees.json")
		df.createOrReplaceTempView("employees")
		val result = sparkSession.sql("SELECT myAverage(salary) as average_salary FROM employees")
		result.show()

		sparkSession.stop()
	}
}
