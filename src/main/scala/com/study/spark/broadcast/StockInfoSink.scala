package com.study.spark.broadcast

import com.stockemotion.common.utils.ObjectUtils
import com.study.spark.config.StockRedisConstants
import com.study.spark.pool.RedisStockInfoClient
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import scala.collection.JavaConversions._

/**
  * Created by piguanghua on 2018/1/23.
  */
object StockInfoSink {
	def broadcastStockInfo(sc:SparkContext): Broadcast[scala.collection.mutable.Map[String, String]] ={
		val stockInfoMap = scala.collection.mutable.Map[String, String]()
		val redisStockInfo = RedisStockInfoClient.getResource()
		val stockCodeString = redisStockInfo.lrange(StockRedisConstants.STOCK_ALL_PAN_CODE, 0 ,-1)
		for(jsonString <-  stockCodeString){
			import com.stockemotion.common.utils.JsonUtils
			val jsonObject = JsonUtils.TO_JSONObject(jsonString)
			val stockCode = ObjectUtils.toString(jsonObject.get("stock_code"))
			val stockName = ObjectUtils.toString(jsonObject.get("stock_name"))
			stockInfoMap += (stockCode->stockName)
		}
		val stockInfoBroadcast:Broadcast[scala.collection.mutable.Map[String, String]] = sc.broadcast(stockInfoMap)
		stockInfoBroadcast
	}




}
