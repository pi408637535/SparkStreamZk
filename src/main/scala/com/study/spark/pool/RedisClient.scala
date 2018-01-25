package com.study.spark.pool

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

/**
  * Created by piguanghua on 2018/1/23.
  */
class RedisClient extends KryoSerializable  {

	import org.apache.commons.pool2.impl.GenericObjectPoolConfig
	import redis.clients.jedis.JedisPool

	var jedisPool: JedisPool = null
	var host: String = null


	def this(host: String) {
		this()
		this.host = host
		jedisPool = new JedisPool(new GenericObjectPoolConfig, host)
	}

	override def read(kryo: Kryo, input: Input) = {
		import org.apache.commons.pool2.impl.GenericObjectPoolConfig
		import redis.clients.jedis.JedisPool
		host = kryo.readObject(input, classOf[String])
		this.jedisPool = new JedisPool(new GenericObjectPoolConfig, host)
	}

	override def write(kryo: Kryo, output: Output) = {
		kryo.writeObject(output, host)
	}
}
