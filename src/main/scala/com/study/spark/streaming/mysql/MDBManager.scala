package com.study.spark.streaming.mysql

import java.sql.Connection
import java.util
import java.util.Properties

import com.alibaba.druid.pool.{DruidAbstractDataSource, DruidDataSource, DruidPooledConnection}
import com.alibaba.druid.proxy.jdbc.TransactionInfo
import com.alibaba.druid.stat.JdbcDataSourceStat
import com.study.spark.config.ConfigurationManager

/**
  * Created by piguanghua on 2017/10/12.
  */

class MDBManager extends Serializable{
	val dataSource:DruidAbstractDataSource = new DruidDataSource()

	try{
	dataSource.setMaxActive(ConfigurationManager.Druid_MaxActive)
	dataSource.setMinIdle(ConfigurationManager.Druid_MinPoolSize)
	dataSource.setMaxIdle(ConfigurationManager.Druid_MaxIdle)
	dataSource.setPoolPreparedStatements(true)
	dataSource.setDriverClassName("com.mysql.jdbc.Driver")
	dataSource.setUrl(ConfigurationManager.DB_URL)
	dataSource.setUsername(ConfigurationManager.JDBC_USERNAME)
	dataSource.setPassword(ConfigurationManager.JDBC_PASSWORD)
	dataSource.setValidationQuery("SELECT 1")
	dataSource.setTestOnBorrow(true)

	}catch {
		case ex:Exception => ex.printStackTrace()
	}

	def getConnection:Connection={
		try{
			dataSource.getConnection
		}catch {
			case ex:Exception => ex.printStackTrace()
				null
		}
	}

}

object MDBManager{

	var mDBManager:MDBManager = _
	def getMDBManager: MDBManager = {
		synchronized{
			if(mDBManager == null){
				mDBManager = new MDBManager();
			}
		}
		mDBManager
	}


}

