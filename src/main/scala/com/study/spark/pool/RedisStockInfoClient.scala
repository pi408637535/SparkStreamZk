package com.study.spark.pool

import com.study.spark.config.ConfigurationManager
import com.study.spark.pool.RedisStockPushClient.pool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * Created by piguanghua on 2017/10/10.
  */
object RedisStockInfoClient {
	val host = ConfigurationManager.STOCK_INFO_HOST
	val port = ConfigurationManager.STOCK_INFO_PORT
	val password = ConfigurationManager.STOCK_INFO_PASSWORD
	val timeOut=ConfigurationManager.REDIS_TIMEOUT
	val db = ConfigurationManager.STOCK_INFO_DATABASE

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

	def getResource(): Jedis ={
		pool.getResource
	}

	def releaseResource(jedis: Jedis) ={
		pool.returnResource(jedis)
	}
}
