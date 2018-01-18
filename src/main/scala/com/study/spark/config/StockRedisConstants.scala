package com.study.spark.config

/**
  * Created by piguanghua on 2017/10/10.
  */
object StockRedisConstants {

	//所有的股票code 大盘的
	val STOCK_ALL_PAN_CODE = "CN:STK:CODELIST:APP"
	//所有股票Code
	//val STOCK_ALL_CODE = "CN:STK:CODELIST:APP:TEST"

	val STOCK_CODE_KEY = "CN:STK:STOCKINFO:*"

	val NORMAL_STOCKCODE_KEY_LEN: Integer = "CN:STK:STOCKINFO:600017.SH".length

	val STOCK_NAME = "stock_name"

	val STOCK_INFO = "CN:STK:STOCKINFO:"

	val STOCK_DATA_CLOSE_STRING = "data_close"

	val STOCK_DATA_SETTLE_STRING = "data_settle"

	val STOCK_DATA_PERCENT_STRING = "data_percent"

	//用于获取最高，最低股价
	val SOTKK_INFO_TICK = "CN:STK:Tick:SNAP:"


	val Unusual_Signal_Stock_Mix = "CN:STK:PUBLISHER:TORATIO"

	val STOCK_EMT = "CHN:STK:EMT:"

	/**
	  * 获取股票实盘基本信息stockinfo
	  */
	val STOCK_INFO_KEY = "CHN:STK:BOARD:STOCKINFO:"

	val STOCK_PRICE = "data_close"

	/**
	  * 股票编码列表
	  */
	val CODE_LIST = "CHN:STOCK:CODELIST"

	val STOCK_WD_CODE = "CHN:STK:EMT:"

	val STROCK_PRICE_LIMIT = "CHN:STK:MARKET:SNAP:"
}
