package com.study.spark.pool

import com.stockemotion.common.utils.DateUtils
import com.study.spark.config.PushRedisConstants
import com.study.spark.streaming.StockIncDownStopCalculate
import com.study.spark.streaming.StockIncDownStopCalculate.getDownStopMessage
import com.study.spark.utils.PushUtils

/**
  * Created by piguanghua on 2017/10/18.
  */
object StockPushTest extends App{
	private def getDownStopMessage(stockName: String, stockCode: String, stockPriceClose: Double, stockPercent: String) =  "小沃盯盘提醒您：您的自选股" + stockName + "(" + stockCode + ")于 " + DateUtils.getCurrentFormatTime("yyyy-MM-dd HH:mm:ss") + "上涨  " +
	  stockPercent + "%， 已达到今日跌停价 " + stockPriceClose + ", 更多信息请点击查看详情。";
	private def getUpStopMessage(stockName: String, stockCode: String, stockPriceClose: Double, stockPercent: String) =  "小沃盯盘提醒您：您的自选股" + stockName + "(" + stockCode + ")于 " + DateUtils.getCurrentFormatTime("yyyy-MM-dd HH:mm:ss") + "下跌  " +
	  stockPercent + "%， 已达到今日跌停价 " + stockPriceClose + ", 更多信息请点击查看详情。";


	val content = getDownStopMessage("平安银行", "000001", 11, "11")
	val pushMessage = getUpStopMessage("平安银行", "1.1", 1, "2")


	PushUtils.sendElfPushMessage("000001" , "平安银行", content, "e45d5e96e565b983a847231f2cfa06a3", pushMessage, 1)

}
