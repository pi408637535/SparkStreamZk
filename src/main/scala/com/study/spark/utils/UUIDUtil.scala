package com.study.spark.utils

import java.util.UUID
/**
  * Created by piguanghua on 2017/8/30.
  */
object UUIDUtil {

  def getUUID64String(): String = (UUID.randomUUID.toString + UUID.randomUUID.toString).replace("-", "")

  def getUUID32String(): String = UUID.randomUUID.toString.replace("-", "")
}
