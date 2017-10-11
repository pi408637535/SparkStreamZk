package com.study.spark.pool

import com.study.spark.config.PushRedisConstants
import scala.collection.JavaConversions._
/**
  * Created by piguanghua on 2017/10/10.
  */
object CheckRedisPool extends App{
	val redisStockPushClient = RedisStockPushClient.pool.getResource
	val userSet = redisStockPushClient.smembers(PushRedisConstants.STOCK_PUSH_ELF_INC_DROP_STOP_USER_SET )
	//val userSet = Set(1,2,3,4)

	userSet.foreach(println(_))
}
