package com.study.spark.utils

/**
  * Created by piguanghua on 2017/8/30.
  */
object StringUtils {
  def fulfuill(str: String): String = if (str.length == 2) str else "0" + str

  def formatDouble(data: Double): String = f"$data%.2f"

}
