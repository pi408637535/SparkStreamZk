package com.study.spark.pool

/**
  * Created by piguanghua on 2017/10/10.
  */
object CheckRedisPool extends App{
	val setString =  RedisStockPushClient.pool.getResource.keys("*")
	println(setString.size())
}
