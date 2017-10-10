package com.study.spark.utils
import spray.json._
import spray.json.DefaultJsonProtocol._
/**
  * Created by piguanghua on 2017/9/13.
  */
object JsonUtils {
	implicit object AnyJsonFormat extends JsonFormat[Any] {
		def write(x: Any): JsValue = x match {
			case n: Int => JsNumber(n)
			case l: Long => JsNumber(l)
			case d: Double => JsNumber(d)
			case f: Float => JsNumber(f.toDouble)
			case s: String => JsString(s)
			case x: Seq[_] => seqFormat[Any].write(x)
			// Get the type of map keys from the first key, translate the rest the same way
			case m: Map[_, _] => if (m.isEmpty) {
				// Translates an emtpy map
				JsObject(Map[String, JsValue]())
			} else {
				m.keys.head match {
					case sym: Symbol =>
						val map = m.asInstanceOf[Map[Symbol, _]]
						val pairs = map.map { case (sym, v) => (sym.name -> write(v))}
						JsObject(pairs)
					case s: String => mapFormat[String, Any].write(m.asInstanceOf[Map[String, Any]])
					case a: Any =>
						val map = m.asInstanceOf[Map[Any, _]]
						val pairs = map.map { case (sym, v) => (sym.toString -> write(v))}
						JsObject(pairs)
				}
			}
			case a: Array[_] => seqFormat[Any].write(a.toSeq)
			case true        => JsTrue
			case false       => JsFalse
			case p: Product  => seqFormat[Any].write(p.productIterator.toSeq)
			case null        => JsNull
			case x           => JsString(x.toString)
		}
		def read(value: JsValue): Any = value match {
			case JsNumber(n) => n.intValue()
			case JsString(s) => s
			case a: JsArray => listFormat[Any].read(value)
			case o: JsObject => mapFormat[String, Any].read(value)
			case JsTrue => true
			case JsFalse => false
			case x => deserializationError("Do not understand how to deserialize " + x)
		}
	}

	def mapToJson(map: Map[String, Any], compact: Boolean = true): String = {
		val jsonAst = map.toJson
		if (compact) jsonAst.compactPrint else jsonAst.prettyPrint
	}

	def listToJson(list: Seq[Any], compact: Boolean = true): String = {
		val jsonAst = list.toJson
		if (compact) jsonAst.compactPrint else jsonAst.prettyPrint
	}

	def mapFromJson(json: String): Map[String, Any] = json.asJson.convertTo[Map[String, Any]]

	def listFromJson(json: String): Seq[Any] = json.asJson.convertTo[Seq[Any]]
}
