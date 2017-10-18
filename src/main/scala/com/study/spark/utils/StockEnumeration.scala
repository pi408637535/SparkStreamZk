package com.study.spark.utils

/**
  * Created by piguanghua on 2017/10/17.
  */
object StockEnumeration  extends Enumeration{
	type StockEnumeration = Value
	val Sell = Value(1)
	val Empty = Value(2)
	val Buy = Value(3)
	val Hold = Value(4)

	val Hold2Sell = Value(41)
	val Buy2Sell = Value(31)
	val Empty2Buy = Value(23)
	val Sell2Buy = Value(13)
	val NOTHING = Value(5)
}
