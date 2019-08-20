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
import java.util

import org.apache.druid.data.input.MapBasedInputRow
import org.apache.druid.java.util.common.granularity.Granularity
import org.apache.druid.java.util.common.{IAE, ISE}
import org.apache.druid.timeline.partition.{HashBasedNumberedShardSpec, ShardSpec}
import org.apache.spark.Partitioner
import org.joda.time.DateTime

import scala.collection.JavaConverters._

class DateBucketAndHashPartitioner(
    @transient var gran: Granularity,
    partMap: Map[(Long, Long), Int],
    optionalDims: Option[Set[String]] = None,
    optionalDimExclusions: Option[Set[String]] = None
)
  extends Partitioner {
  lazy val shardLookups = partMap.groupBy { case ((bucket, _), _) => bucket }
    .map { case (bucket, indices) => bucket -> indices.size }
    .mapValues(maxPartitions => {
      val shardSpecs = (0 until maxPartitions)
        .map(new HashBasedNumberedShardSpec(_, maxPartitions, null, SerializedJsonStatic.mapper).asInstanceOf[ShardSpec])
      shardSpecs.head.getLookup(shardSpecs.asJava)
    })
  lazy val excludedDimensions = optionalDimExclusions.getOrElse(Set())

  override def numPartitions: Int = partMap.size

  override def getPartition(key: Any): Int = key match {
    case (k: Long, v: AnyRef) =>
      val eventMap: Map[String, AnyRef] = v match {
        case mm: util.Map[_, _] =>
          mm.asInstanceOf[util.Map[String, AnyRef]].asScala.toMap
        case mm: Map[_, _] =>
          mm.asInstanceOf[Map[String, AnyRef]]
        case x =>
          throw new IAE(s"Unknown value type ${x.getClass} : [$x]")
      }
      val dateBucket = gran.bucketStart(new DateTime(k)).getMillis
      val shardLookup = shardLookups.get(dateBucket) match {
        case Some(sl) => sl
        case None => throw new IAE(s"Bad date bucket $dateBucket")
      }

      val shardSpec = shardLookup.getShardSpec(
        dateBucket,
        new MapBasedInputRow(
          dateBucket,
          optionalDims.getOrElse(eventMap.keySet -- excludedDimensions).toList.asJava,
          eventMap.asJava
        )
      )
      val timePartNum = shardSpec.getPartitionNum

      partMap.get((dateBucket.toLong, timePartNum)) match {
        case Some(part) => part
        case None => throw new ISE(s"bad date and partition combo: ($dateBucket, $timePartNum)")
      }
    case x => throw new IAE(s"Unknown type ${x.getClass} : [$x]")
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    out.writeObject(new SerializedJson[Granularity](gran))
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    gran = in.readObject().asInstanceOf[SerializedJson[Granularity]].delegate
  }
}
