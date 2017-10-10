package com.study.spark.spark.session

import com.study.spark.constant.Constants
import org.apache.spark.sql.SparkSession

/**
  * Created by piguanghua on 2017/8/30.
  */
object UserVisitSessionAnalyzeSpark {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName(Constants.SPARK_APP_NAME_SESSION)
      .master("local")
      .config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse")
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext
    import com.study.spark.dao.MockData
    MockData.sparkSqlTestRDDRealData(sparkContext, sparkSession)

    
  }
}
