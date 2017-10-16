package com.study.spark.pool

import java.sql.{Date, Timestamp}

import com.study.spark.streaming.mysql.{MDBManager, SparkConnectionPool, SparkPushConnectionPool}
import com.study.spark.utils.TimeUtils

/**
  * Created by piguanghua on 2017/10/10.
  */
object CheckMysqlPool extends App{


	val connPush = MDBManager.getMDBManager.getConnection
	val sqlPush = "insert into kafka_data_log(stock_code,stock_price,stock_price_high,stock_price_low,sys_create_time) "+ "values('"  + "000001.SZ" +"'" + "," + 1 + "," + 1  + ","+ 1 + ","+    "'" + TimeUtils.getCurrent_time() +"'" + ")"
	val stmtPush = connPush.createStatement()
	stmtPush.executeUpdate(sqlPush)
	connPush.close()
}
