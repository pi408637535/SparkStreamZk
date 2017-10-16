package com.study.spark.config

import com.stockemotion.common.utils.{ObjectUtils, PropertiesGetter}

/**
  * Created by piguanghua on 2017/8/29.
  */
object ConfigurationManager {

	// mysql
	val DB_DRIVER = ObjectUtils.toString(PropertiesGetter.getValue("db.driver"))
	val DB_URL = ObjectUtils.toString(PropertiesGetter.getValue("jdbc.url"))
	val JDBC_USERNAME = ObjectUtils.toString(PropertiesGetter.getValue("jdbc.username"))
	val JDBC_PASSWORD = ObjectUtils.toString(PropertiesGetter.getValue("jdbc.password"))

	val JDBC_ConnectionNum = ObjectUtils.toInteger(PropertiesGetter.getValue("jdbc.connectionNum"))
	val JDBC_MAX = ObjectUtils.toInteger(PropertiesGetter.getValue("jdbc.max"))

	val Druid_InitialSize = ObjectUtils.toInteger(PropertiesGetter.getValue("druid.initialSize"))
	val Druid_MinPoolSize = ObjectUtils.toInteger(PropertiesGetter.getValue("druid.minPoolSize"))
	val Druid_MaxIdle = ObjectUtils.toInteger(PropertiesGetter.getValue("druid.maxIdle"))
	val Druid_MaxActive = ObjectUtils.toInteger(PropertiesGetter.getValue("druid.maxActive"))



	// pool
	val REDIS_MAX_IDLE = ObjectUtils.toInteger(PropertiesGetter.getValue("redis.max.idle"))
	val REDIS_MAX_TOTAL = ObjectUtils.toInteger(PropertiesGetter.getValue("redis.max.total"))
	val REDIS_timeBetweenEvictionRunsMillis = ObjectUtils.toLong(PropertiesGetter.getValue("redis.timeBetweenEvictionRunsMillis"))
	val REDIS_minEvictableIdleTimeMillis = ObjectUtils.toLong(PropertiesGetter.getValue("redis.minEvictableIdleTimeMillis"))
	val REDIS_TIMEOUT = ObjectUtils.toInteger(PropertiesGetter.getValue("redis.timeout"))


	//redis
	val STOCK_INFO_HOST=ObjectUtils.toString(PropertiesGetter.getValue("stock.info.host"))
	val STOCK_INFO_PORT=ObjectUtils.toInteger(PropertiesGetter.getValue("stock.info.port"))
	val STOCK_INFO_PASSWORD=ObjectUtils.toString(PropertiesGetter.getValue("stock.info.password"))
	val STOCK_INFO_DATABASE=ObjectUtils.toInteger(PropertiesGetter.getValue("stock.info.database"))


	val  STOCK_WD_HOST=ObjectUtils.toString(PropertiesGetter.getValue("stock.wd.host"))
	val STOCK_WD_PORT=ObjectUtils.toInteger(PropertiesGetter.getValue("stock.wd.port"))
	val STOCK_WD_PASSWORD=ObjectUtils.toString(PropertiesGetter.getValue("stock.wd.password"))
	val STOCK_WD_DATABASE=ObjectUtils.toInteger(PropertiesGetter.getValue("stock.wd.database"))

	val  REDIS_PUSH_HOST=ObjectUtils.toString(PropertiesGetter.getValue("redis.push.host"))
	val REDIS_PUSH_PORT=ObjectUtils.toInteger(PropertiesGetter.getValue("redis.push.port"))
	val REDIS_PUSH_PASSWORD=ObjectUtils.toString(PropertiesGetter.getValue("redis.push.password"))
	val REDIS_PUSH_DATABASE=ObjectUtils.toInteger(PropertiesGetter.getValue("redis.push.database"))

	//个推
	val GETXIN_APPID = ObjectUtils.toInteger(PropertiesGetter.getValue("getxin.appid"))
	val GETXIN_APPKEY = ObjectUtils.toInteger(PropertiesGetter.getValue("getxin.appkey"))
	val GETXIN_MASTER_SECRET = ObjectUtils.toInteger(PropertiesGetter.getValue("getxin.master.secret"))
	val GETXIN_HOST =ObjectUtils.toInteger(PropertiesGetter.getValue("getxin.host"))
	val GETXIN_SECRET = ObjectUtils.toInteger(PropertiesGetter.getValue("getxin.secret"))


	val MESSAGE_URL = ObjectUtils.toString(PropertiesGetter.getValue("message_url"))
}
