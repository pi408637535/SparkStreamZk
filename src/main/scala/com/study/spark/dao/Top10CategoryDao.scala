package com.study.spark.dao

import com.study.spark.slick.Tables._
import slick.lifted.Tag
/**
  * Created by piguanghua on 2017/8/30.
  */
case class Top10CategoryDao(val _tableTag: Tag) extends Top10CategorySession(_tableTag)