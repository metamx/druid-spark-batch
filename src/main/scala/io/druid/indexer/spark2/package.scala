package io.druid.indexer

import java.util.{Map => jMap}

import io.druid.segment.ProgressIndicator
import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.joda.time.format.DateTimeFormatter

package object spark2 {

  type GranularizedDruidRow = (Long, jMap[String,AnyRef])
  type DruidIndexableRow = ((Long, jMap[String, AnyRef]), Int)

  def toDruid(dT : DataType, v : Any, isNull : Boolean)
             (implicit dtFmt : DateTimeFormatter) : AnyRef = (dT, v, isNull) match {
    case (_, _, true) => null
    case (BooleanType, v, _) => java.lang.Boolean.valueOf(v.asInstanceOf[Boolean])
    case (ByteType, v, _) => java.lang.Byte.valueOf(v.asInstanceOf[Byte])
    case (ShortType, v, _) => java.lang.Short.valueOf(v.asInstanceOf[Short])
    case (IntegerType, v, _) => java.lang.Integer.valueOf(v.asInstanceOf[Int])
    case (LongType, v, _) => java.lang.Long.valueOf(v.asInstanceOf[Long])
    case (FloatType, v, _) => java.lang.Float.valueOf(v.asInstanceOf[Float])
    case (DoubleType, v, _) => new java.lang.Float(
      java.lang.Double.valueOf(v.asInstanceOf[Double]).toFloat)
    case (t:DecimalType, v, _) => new java.lang.Float(v.asInstanceOf[BigDecimal].toFloat)
    case (DateType, v, _) => dtFmt.print(v.asInstanceOf[java.sql.Date].getTime)
    case (TimestampType, v, _) => dtFmt.print(v.asInstanceOf[java.sql.Date].getTime)
    case (StringType, v,_) => v.toString
    case (aT:ArrayType, v, _) =>
      scala.collection.JavaConversions.seqAsJavaList(
        v.asInstanceOf[Seq[_]].map(toDruid(aT.elementType, _, false))
      )
    case (mT:MapType, v, _) => scala.collection.JavaConversions.mapAsJavaMap(
      v.asInstanceOf[Map[_,_]].map {
        case (k, v) => toDruid(mT.keyType, k, false) -> toDruid(mT.keyType, v, false)
      }
    )
    case (sT:StructType, v, _) => mapRow(sT, v.asInstanceOf[Row])
    case (x, _, _) => throw new RuntimeException(s"Conversion of type $x to Druid not supported")
  }

  def mapRow(schema : StructType, r : Row)
            (implicit dtFmt : DateTimeFormatter): Map[String, AnyRef] = {
    schema.fields.zipWithIndex.map {
      case (f,i) => f.name -> toDruid(f.dataType, r.get(i), r.isNullAt(i))
    }.toMap
  }

  implicit def fromDelegate[T](i : SerializedJson[T]) : T = i.getDelegate
  implicit def toDelegate[T](i : T) : SerializedJson[T] = new SerializedJson[T](i)


  object PI extends ProgressIndicator with Logging {
    override def stop(): Unit = {
      log.trace("Stop")
    }

    override def stopSection(s: String): Unit = {
      if (log.isTraceEnabled) log.trace("%s", "Stop [%s]" format s)
    }

    override def progress(): Unit = {
      log.trace("Progress")
    }

    override def startSection(s: String): Unit = {
      if (log.isTraceEnabled) log.trace("%s", "Start [%s]" format s)
    }

    override def progressSection(s: String, s1: String): Unit = {
      if (log.isTraceEnabled) log.trace("%s", "Start [%s]:[%s]" format(s, s1))
    }

    override def start(): Unit = {
      log.trace("Start")
    }
  }

}