/*
 *  Licensed to Metamarkets Group Inc. (Metamarkets) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  Metamarkets licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.druid.indexer.spark

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import org.apache.druid.java.util.common.granularity.{Granularity, GranularityType, PeriodGranularity}
import org.apache.druid.java.util.common.{IAE, ISE}
import org.apache.spark.Partitioner
import org.joda.time.{DateTime, Interval, Period}

class DateBucketPartitioner(@transient var gran: Granularity, intervals: Iterable[Interval]) extends Partitioner {
  val intervalMap: Map[Long, Int] = intervals
    .map(_.getStart.getMillis)
    .foldLeft(Map[Long, Int]())((a, b) => a + (b -> a.size))

  override def numPartitions: Int = intervalMap.size

  override def getPartition(key: Any): Int = key match {
    case null => throw new NullPointerException("Bad partition key")
    case (k: Long, _: Any) => getPartition(k)
    case k: Long =>
      val mapKey = gran.bucketStart(new DateTime(k)).getMillis
      val v = intervalMap.get(mapKey)
      if (v.isEmpty) {
        // Lazy ISE creation. getOrElse will create it each time
        throw new ISE(
          s"unknown bucket for datetime $mapKey. Known values are ${intervalMap.keys}"
        )
      }
      v.get
    case x => throw new IAE(s"Unknown type for $x")
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    val granularity = gran.asInstanceOf[PeriodGranularity]
    if (GranularityType.isStandard(gran)) {
      out.writeObject(new SerializedJson[String](granularity.getPeriod.toString))
    } else {
      out.writeObject(new SerializedJson[Granularity](granularity))
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    val value = in.readObject()
    gran = value match {
      case s: SerializedJson[_] =>
        s.delegate match {
          case str: String => new PeriodGranularity(new Period(str), null, null)
          case x : Granularity => x
        }
    }
  }
}
