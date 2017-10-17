package com.study.spark.spark.session
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by piguanghua on 2017/10/16.
  */
object reduceByWindowOnStreaming {
	def main(args: Array[String]) {
		/**
		  * this is test  of Streaming operations-----reduceByKeyAndWindow
		  */
		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
		Logger.getLogger("org.eclipse.jetty.Server").setLevel(Level.OFF)

		val conf = new SparkConf().setAppName("the reduceByWindow operation of SparK Streaming").setMaster("local[2]")
		val sc = new SparkContext(conf)
		val ssc = new StreamingContext(sc,Seconds(2))

		//set the Checkpoint directory
		ssc.checkpoint("/tmp/sparkstreaming")

		//get the socket Streaming data
		val socketStreaming = ssc.socketTextStream("localhost",9999)

		val data = socketStreaming.map(x =>(x,1))
		//def reduceByKeyAndWindow(reduceFunc: (V, V) => V, windowDuration: Duration  ): DStream[(K, V)]
		//val getedData1 = data.reduceByKeyAndWindow(_+_,Seconds(6))

		val getedData2 = data.reduceByKeyAndWindow(_+_,
			(a,b) => a+b*0
			,Seconds(6),Seconds(2))

		val getedData1 = data.reduceByKeyAndWindow(_+_,_-_,Seconds(9),Seconds(6))

		println("reduceByKeyAndWindow : ")
		getedData1.print()

		ssc.start()
		ssc.awaitTermination()


	}
}
