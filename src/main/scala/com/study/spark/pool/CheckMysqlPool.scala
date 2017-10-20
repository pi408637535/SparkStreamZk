package com.study.spark.pool

import java.sql.{Date, Timestamp}

import com.stockemotion.common.utils.ObjectUtils
import com.study.spark.streaming.mysql.{MDBManager, SparkConnectionPool, SparkPushConnectionPool}
import com.study.spark.utils.TimeUtils

/**
  * Created by piguanghua on 2017/10/10.
  */
object CheckMysqlPool extends App{


	val connPush = MDBManager.getMDBManager.getConnection
	val sqlPush = "insert into push_error_log(stock_code,user_id,drop_price,sys_create_time) "+ "values(    '"  +   "000001.SZ"  +"'" + "," + 1  + "," +  1  + "," + "'" + TimeUtils.getCurrent_time() +"'" + ")"
	val stmtPush = connPush.createStatement()
	stmtPush.executeUpdate(sqlPush)
	connPush.close()
}
