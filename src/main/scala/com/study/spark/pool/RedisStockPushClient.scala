package com.study.spark.pool

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * Created by piguanghua on 2017/10/10.
  */
object RedisStockPushClient {
	val host = "118.244.212.178"
	val port = 16379
	val password = "password123"
	val db = 0
	val timeOut=30000
	//定义pool
	val genericObjectPoolConfig  = new GenericObjectPoolConfig()
	genericObjectPoolConfig.setMaxTotal(2000)
	genericObjectPoolConfig.setMaxIdle(1000)
	genericObjectPoolConfig.setTimeBetweenEvictionRunsMillis(1000)
	genericObjectPoolConfig.setMinEvictableIdleTimeMillis(1000)
	lazy val pool =new JedisPool(genericObjectPoolConfig,host,port,timeOut, password, db)
	//关闭连接池
	lazy val hook =new Thread{
		override def run={
			pool.destroy()
		}
	}
	sys.addShutdownHook(hook.run)
}
