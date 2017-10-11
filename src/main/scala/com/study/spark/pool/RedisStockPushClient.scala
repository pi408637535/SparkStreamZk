package com.study.spark.pool

import com.study.spark.config.ConfigurationManager
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * Created by piguanghua on 2017/10/10.
  */
object RedisStockPushClient {
	val host = ConfigurationManager.REDIS_PUSH_HOST
	val port = ConfigurationManager.REDIS_PUSH_PORT
	val password = ConfigurationManager.REDIS_PUSH_PASSWORD
	val db = ConfigurationManager.REDIS_PUSH_DATABASE

	val timeOut=ConfigurationManager.REDIS_TIMEOUT
	//定义pool
	val genericObjectPoolConfig  = new GenericObjectPoolConfig()
	genericObjectPoolConfig.setMaxTotal(ConfigurationManager.REDIS_MAX_TOTAL)
	genericObjectPoolConfig.setMaxIdle(ConfigurationManager.REDIS_MAX_IDLE)
	genericObjectPoolConfig.setTimeBetweenEvictionRunsMillis(ConfigurationManager.REDIS_timeBetweenEvictionRunsMillis)
	genericObjectPoolConfig.setMinEvictableIdleTimeMillis(ConfigurationManager.REDIS_minEvictableIdleTimeMillis)

	lazy val pool =new JedisPool(genericObjectPoolConfig, host, port, timeOut, password, db)
	//关闭连接池
	lazy val hook =new Thread{
		override def run={
			pool.destroy()
		}
	}
	sys.addShutdownHook(hook.run)
}
