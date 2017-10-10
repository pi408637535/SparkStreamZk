package com.study.spark.pool.kafka

import java.sql.{Date, Timestamp}

import com.study.spark.streaming.mysql.ConnectionPool

/**
  * Created by piguanghua on 2017/10/10.
  */
object CheckMysqlPool extends App{
	val conn = ConnectionPool.getJdbcConn
	val sql = "insert into stock_state(stock_name,stockPercent,userPercentSetting,state,time) "+ "values('" + "300701.SZ" +"'" + "," + "2" + ","+ "1" + ","+  1 +","+   "'" + new Timestamp(new Date(System.currentTimeMillis()).getTime) +"'" + ")"
	val stmt = conn.createStatement()
	stmt.executeUpdate(sql)
}
