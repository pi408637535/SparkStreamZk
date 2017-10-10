package com.study.spark.pool

import java.io.UnsupportedEncodingException

import com.edi.kafka.messages.{MessageEncoder, MessageSender, MessageSenderPool}

/**
  * Created by piguanghua on 2017/9/25.
  */
object KafkaPoolUtils {

	private val kafkaPool: MessageSenderPool =  new MessageSenderPool(Runtime.getRuntime().availableProcessors()*2+1,
		"stockPercentResult",
	//	"192.168.152.137:9092,192.168.152.160:9092,192.168.152.163:9092")
		"192.168.1.226:9092,192.168.1.161:9092,192.168.1.227:9092")


	def getKafkaSender()={
		val sender = kafkaPool.getSender(2000)
		sender
	}

	def getStringEncoder()={
		new MessageEncoder[String] {
			override def encode(msg: String): Array[Byte] = {
				val ret = msg.getBytes("UTF-8")
				ret;
			}
		}
	}

	def returnSender(sender:MessageSender): Unit ={
		kafkaPool.returnSender(sender)
	}



}
