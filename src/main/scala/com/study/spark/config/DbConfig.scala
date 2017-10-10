package com.study.spark.config
import slick.driver.MySQLDriver.api._

import scala.concurrent.Await
import scala.concurrent.duration._
/**
  * Created by piguanghua on 2017/8/30.
  */
object DbConfig {
  def exec[T](program: DBIO[T]): T = Await.result(db.run(program), 2.seconds)
  val db = Database.forConfig("mysql")
}
