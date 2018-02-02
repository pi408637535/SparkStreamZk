package com.study.spark.utils
import java.text.SimpleDateFormat
import java.util.Date
/**
  * Created by piguanghua on 2017/10/11.
  */
object TimeUtils {
	val now = new Date()
	def getCurrent_time(): Long = {
		val a = now.getTime
		val str = a+""
		str.toLong
	}
	def getZero_time():Long={
		val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
		val a = dateFormat.parse(dateFormat.format(now)).getTime
		val str = a+""
		str.substring(0,10).toLong
	}

	def main(args: Array[String]): Unit = {
		println(getCurrent_time())
	}
}
