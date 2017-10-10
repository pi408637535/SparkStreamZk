package com.study.spark.dao

import java.util

import com.study.spark.utils.{DateUtil, StringUtils, UUIDUtil}
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by piguanghua on 2017/8/30.
  */
case class PersonLog(val date: String, val userId: Int, sessionId: String, val pageId: Int,
			   val actionTime: String, val searchKeyword: String, val clickCategoryId: Long,
			   val clickProductId: Long, val orderCategoryIds: String, val orderProductIds: String,
			   val payCategoryIds: String, val payProductIds: String)

object MockData extends App {


	def createData(): ArrayBuffer[PersonLog] = {
		val rows = scala.collection.mutable.ArrayBuffer.empty[PersonLog]
		val searchKeywords = List("火锅", "蛋糕", "重庆辣子鸡", "重庆小面", "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉")
		val actions = List("search", "click", "order", "pay")
		val date = DateUtil.getDateString()
		var random: Random = Random

		for (i <- (1 to 100)) {
			val userId = random.nextInt(i)
			val sessionId = UUIDUtil.getUUID32String()
			val baseActionTime = date + " " + random.nextInt(23)

			for (j <- (1 to random.nextInt(100))) {
				val pageId = random.nextInt(10)
				val actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)))
				//String actionTime
				var searchKeyword: String = ""
				var clickCategoryId: Long = 0L
				var clickProductId: Long = 0L
				var orderCategoryIds: String = ""
				var orderProductIds: String = ""
				var payCategoryIds: String = ""
				var payProductIds: String = ""
				val action = actions(random.nextInt(4))

				if ("search" == action) searchKeyword = searchKeywords(random.nextInt(10))
				else if ("click" == action) {
					clickCategoryId = random.nextInt(100)
					clickProductId = random.nextInt(100)
				}
				else if ("order" == action) {
					orderCategoryIds = String.valueOf(random.nextInt(100))
					orderProductIds = String.valueOf(random.nextInt(100))
				}
				else if ("pay" == action) {
					payCategoryIds = String.valueOf(random.nextInt(100))
					payProductIds = String.valueOf(random.nextInt(100))
				}

				var persons = PersonLog(date, userId, sessionId,
					pageId, actionTime, searchKeyword,
					clickCategoryId, clickProductId,
					orderCategoryIds, orderProductIds,
					payCategoryIds, payProductIds);

				rows += persons

			}
		}
		rows
	}

	def sparkSqlTestRDDRealData(sparkContext: SparkContext, sparkSession: SparkSession): Unit = {
		val rows = MockData.createData().toArray
		val rowsRDD = sparkContext.parallelize(rows)

	//	rowsRDD.foreach(println(_))


		val struct:StructType = StructType(
			StructField("date", StringType, true )::
			  StructField("userId", IntegerType, true) ::
			  StructField("sessionId", StringType, true) ::
			  StructField("pageId", StringType, true) ::
			  StructField("actionTime", StringType, true) ::
			  StructField("searchKeyword", StringType, true) ::
			  StructField("clickCategoryId", LongType, true) ::
			  StructField("clickProductId", LongType, true) ::
			  StructField("orderCategoryIds", StringType, true) ::
			  StructField("orderProductIds", StringType, true) ::
			  StructField("payCategoryIds", StringType, true) ::
			  StructField("payProductIds", StringType, true) ::
			   Nil
		)


		val schema = StructType(util.Arrays.asList(
			DataTypes.createStructField("date", DataTypes.StringType, true),
			DataTypes.createStructField("user_id", DataTypes.LongType, true),
			DataTypes.createStructField("session_id", DataTypes.StringType, true),
			DataTypes.createStructField("page_id", DataTypes.LongType, true),
			DataTypes.createStructField("action_time", DataTypes.StringType, true),
			DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
			DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
			DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
			DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
			DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
			DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
			DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true)));
		import sparkSession.implicits._
		val caseClassDS = rowsRDD.toDS()

		caseClassDS.show()
	//	val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

	//	val rowRDD = rdd.map(value => Row.fromSeq(List(value)))

		import sparkSession.implicits._
	//	val rddToDF = sparkSession.createDataFrame(rowsRDD, struct);
//		val rDDToDataSet = rddToDF.as[org.apache.spark.sql.Row]

//		rDDToDataSet.show()
	}

}
