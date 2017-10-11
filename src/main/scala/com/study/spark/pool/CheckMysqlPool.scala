package com.study.spark.pool

import java.sql.{Date, Timestamp}

import com.study.spark.streaming.mysql.{SparkConnectionPool, SparkPushConnectionPool}
import com.study.spark.utils.TimeUtils

/**
  * Created by piguanghua on 2017/10/10.
  */
object CheckMysqlPool extends App{
	val connPush= SparkPushConnectionPool.getJdbcConn
	val sqlPush = "insert into push_log(stock_code,user_id,inc_percent,percent_now,sys_create_time) "+ "values('"  + "300001.SZ" +"'" + "," + 11 +  ","+ 12 + ","+ 15 + ","+    "'" + TimeUtils.getCurrent_time() +"'" + ")"
	val stmtPush = connPush.createStatement()
	stmtPush.executeUpdate(sqlPush)
	SparkPushConnectionPool.releaseConn(connPush)
}
