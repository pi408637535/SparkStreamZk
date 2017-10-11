package com.study.spark.streaming.mysql

import java.sql.{Connection, DriverManager}
import java.util

import com.study.spark.config.ConfigurationManager

/**
  * Created by piguanghua on 2017/10/10.
  */
object SparkPushConnectionPool {
	private val max = ConfigurationManager.JDBC_MAX                         //连接池连接总数
	private val connectionNum = ConfigurationManager.JDBC_CONNECTIONNum    //每次产生连接数
	private var conNum = 0                  //当前连接池已产生的连接数
	private val pool = new util.LinkedList[Connection]()    //连接池

	//加载驱动
	private def preGetConn() : Unit = {
		//控制加载
		if (conNum < max && !pool.isEmpty) {
			println("Jdbc Pool has no connection now, please wait a moments!")
			Thread.sleep(2000)
			preGetConn()
		} else {
			Class.forName(ConfigurationManager.DB_DRIVER);
		}
	}

	def getJdbcConn() : Connection = {
		//同步代码块
		AnyRef.synchronized({
			if (pool.isEmpty) {
				//加载驱动
				preGetConn()
				for (i <- 1 to connectionNum) {
					val conn = DriverManager.getConnection(ConfigurationManager.DB_URL, ConfigurationManager.JDBC_USERNAME, ConfigurationManager.JDBC_PASSWORD)
					pool.push(conn)
					conNum += 1
				}
			}
			pool.poll()
		})
	}

	//释放连接
	def releaseConn(conn:Connection): Unit ={
		pool.push(conn)
	}
	//加载驱

}
