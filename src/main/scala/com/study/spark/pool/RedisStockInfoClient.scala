package com.study.spark.pool

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * Created by piguanghua on 2017/10/10.
  */
object RedisStockInfoClient {
	val host = "192.168.1.236"
	val port = 47614
	val password = "Hezhiyu2Cuiping"
	val timeOut=30000
	//定义pool
	val genericObjectPoolConfig  = new GenericObjectPoolConfig()
	genericObjectPoolConfig.setMaxTotal(2000)
	genericObjectPoolConfig.setMaxIdle(1000)
	genericObjectPoolConfig.setTimeBetweenEvictionRunsMillis(1000)
	genericObjectPoolConfig.setMinEvictableIdleTimeMillis(1000)
	lazy val pool =new JedisPool(genericObjectPoolConfig,host,port,timeOut, password)
	//关闭连接池
	lazy val hook =new Thread{
		override def run={
			pool.destroy()
		}
	}
	sys.addShutdownHook(hook.run)
}
