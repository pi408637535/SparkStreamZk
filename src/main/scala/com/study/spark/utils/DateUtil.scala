package com.study.spark.utils

import java.text.SimpleDateFormat
import java.util.Date
/**
  * Created by piguanghua on 2017/8/30.
  */
object DateUtil {
  private final val partternAll = "yyyy-MM-dd: HH:mm:ss"

  private final val partternPart = "yyyy-MM-dd"

  def getDateString(): String = new SimpleDateFormat(partternPart).format(new Date)

  def getDateTimeString(parttern: String): String = new SimpleDateFormat(parttern).format(new Date)

  def getDateTimeString(): String = new SimpleDateFormat(partternAll).format(new Date)

  def getDate(): Date = new SimpleDateFormat(partternPart).parse(getDateString)

  def getDateTime: Date = new SimpleDateFormat(partternAll).parse(getDateTimeString)

  def getLongDateTime(): Long = new Date().getTime
}
