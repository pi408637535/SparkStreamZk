package com.study.spark.utils

import com.alibaba.fastjson.JSONObject
import com.study.spark.config.ConfigurationManager

/**
  * Created by piguanghua on 2017/10/11.
  */
object WodeInfoUtils {
	def message(userId:String, title:String, content:String, jsonData:JSONObject)={
		import com.stockemotion.common.utils.DateUtils
		import com.stockemotion.common.utils.HttpClientUtil
		import java.io.IOException
		val jsonObject = new JSONObject()
		val message = new JSONObject()
		jsonObject.put("userId", userId)
		jsonObject.put("code", 1)
		val dateString = DateUtils.getCurrentTimestamp.toString
		message.put("date", dateString.substring(0, dateString.length - 5))
		message.put("title", title)
		message.put("content", content)
		message.put("data", jsonData)
		jsonObject.put("message", message)

		try
			HttpClientUtil.httpPost(ConfigurationManager.MESSAGE_URL, jsonObject)
		catch {
			case e: IOException =>
				e.printStackTrace()
		}
	}
}
