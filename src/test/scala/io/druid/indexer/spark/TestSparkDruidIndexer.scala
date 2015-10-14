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

package io.druid.indexer.spark

import java.io.Closeable
import java.nio.file.Files

import com.google.common.io.Closer
import com.metamx.common.logger.Logger
import com.metamx.common.{Granularity, IAE}
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Interval}
import org.scalatest._

import scala.collection.JavaConverters._


class TestSparkDruidIndexer extends FlatSpec with Matchers
{

  import TestScalaBatchIndexTask._

  "The spark indexer" should "return proper DataSegments" in {
    val data_file = this.getClass.getResource("/lineitem.small.tbl").toString
    val closer = Closer.create()
    val outDir = Files.createTempDirectory("segments").toFile
    (outDir.mkdirs() || outDir.exists()) && outDir.isDirectory should be(true)
    closer.register(
      new Closeable()
      {
        override def close(): Unit = FileUtils.deleteDirectory(outDir)
      }
    )
    try {
      val conf = new SparkConf()
        .setAppName("Simple Application")
        .setMaster("local[4]")
        .set("user.timezone", "UTC")
        .set("file.encoding", "UTF-8")
        .set("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager")
        .set("org.jboss.logging.provider", "slf4j")
        .set("druid.processing.columnCache.sizeBytes", "1000000000")
        .set("spark.driver.host", "localhost")
        .set("spark.executor.userClassPathFirst", "true")
        .set("spark.driver.userClassPathFirst", "true")
        .set("spark.kryo.referenceTracking", "false")
        .registerKryoClasses(SparkBatchIndexTask.KRYO_CLASSES)

      val sc = new SparkContext(conf)
      closer.register(
        new Closeable
        {
          override def close(): Unit = sc.stop()
        }
      )
      val loadResults = SparkDruidIndexer.loadData(
        Seq(data_file),
        new SerializedJson(dataSchema),
        interval,
        rowsPerPartition,
        rowsPerFlush,
        outDir.toString,
        indexSpec,
        sc
      )
      loadResults.length should be(7)
      val dataSegment = loadResults.head
      dataSegment.getBinaryVersion should be(9)
      dataSource should equal(dataSegment.getDataSource)
    }
    finally {
      closer.close()
    }
  }

  "The DateBucketPartitioner" should "properly partition single item data" in {
    val interval = Interval.parse("1992/1993")
    val partitioner = new DateBucketPartitioner(Granularity.YEAR, interval)
    partitioner.getPartition((interval.getStartMillis, 0)) should equal(0L)
  }


  "The DateBucketPartitioner" should "throw an error if out of bounds" in {
    val interval = Interval.parse("1992/1993")
    val partitioner = new DateBucketPartitioner(Granularity.YEAR, interval)
    an[IAE] should be thrownBy {
      partitioner.getPartition((0, 0))
    }
  }

  "The DateBucketPartitioner" should "properly partition for multiple timespans" in {
    val interval = Interval.parse("1992/1999")
    val partitioner = new DateBucketPartitioner(Granularity.YEAR, interval)
    var idex = 0
    Granularity.YEAR.getIterable(interval).asScala.foreach(
      i => {
        partitioner.getPartition((i.getStartMillis, 0)) should equal(idex)
        idex += 1
      }
    )
  }

  "The DateBucketAndHashPartitioner" should "properly partition single data" in {
    val interval = Interval.parse("1992/1993")
    val partitioner = new
        DateBucketAndHashPartitioner(Granularity.YEAR, interval, Map((new DateTime("1992").getMillis, 0L) -> 0))
    partitioner.getPartition(makeEvent(interval.getStart)) should equal(0)
  }


  "The DateBucketAndHashPartitioner" should "properly partition multiple date ranges" in {
    val interval = Interval.parse("1992/1994")
    val partitioner = new DateBucketAndHashPartitioner(
      Granularity.YEAR,
      interval,
      Map((new DateTime("1992").getMillis, 0L) -> 0, (new DateTime("1993").getMillis, 0L) -> 1)
    )
    partitioner.getPartition(makeEvent(interval.getStart)) should equal(0)
    partitioner.getPartition(makeEvent(interval.getEnd.minus(10L))) should equal(1)
  }

  "The DateBucketAndHashPartitioner workflow" should "properly partition multiple date ranges and buckets" in {
    val interval = Interval.parse("1992/1994")
    val map = Map(new DateTime("1992").getMillis -> 100L, new DateTime("1993").getMillis -> 200L)
    val m = SparkDruidIndexer.getSizedPartitionMap(map, 150)
    val partitioner = new DateBucketAndHashPartitioner(Granularity.YEAR, interval, m)
    partitioner.getPartition(makeEvent(interval.getStart)) should equal(0)
    partitioner.getPartition(makeEvent(interval.getEnd.minus(10L))) should equal(1)
    partitioner.getPartition(makeEvent(interval.getEnd.minus(10L), "something else")) should equal(2)
    partitioner.getPartition(makeEvent(interval.getEnd.minus(10L), "anotherHash3")) should equal(1)
    partitioner.getPartition(makeEvent(interval.getEnd.minus(10L), "anotherHash")) should equal(2)
  }

  def makeEvent(d: DateTime, v: String = "dim11"): (Long, Set[(String, Set[String])]) = {
    (d.getMillis, Set(("dim1", Set(v))))
  }

  "getSizedPartitionMap" should "partition data correctly for single items" in {
    val map = Map(0L -> 100L)
    val m = SparkDruidIndexer.getSizedPartitionMap(map, 1000)
    m.size should equal(1)
    m.get((0L, 0L)) should equal(Some(0L))
  }

  "getSizedPartitionMap" should "return nothing if unknown coordinates" in {
    val map = Map(0L -> 100L)
    val m = SparkDruidIndexer.getSizedPartitionMap(map, 1000)
    m.size should equal(1)
    m.get((1L, 0L)) should be('empty)
  }

  "getSizedPartitionMap" should "partition data correctly for single boundary counts" in {
    val map = Map(0L -> 100L)
    val m = SparkDruidIndexer.getSizedPartitionMap(map, 100)
    m.size should equal(2)
    m.get((0L, 0L)) should equal(Some(0L))
    m.get((0L, 1L)) should equal(Some(1L))
  }

  "getSizedPartitionMap" should "partition data correctly for multiple date buckets" in {
    val map = Map(0L -> 100L, 1L -> 100L)
    val m = SparkDruidIndexer.getSizedPartitionMap(map, 1000)
    m.size should equal(2)
    m.get((0L, 0L)) should equal(Some(0L))
    m.get((1L, 0L)) should equal(Some(1L))
  }

  "getSizedPartitionMap" should "ignore empty intervals" in {
    val map = Map(0L -> 100L, 1L -> 0L)
    val m = SparkDruidIndexer.getSizedPartitionMap(map, 1000)
    m.size should equal(1)
    m.get((0L, 0L)) should equal(Some(0L))
  }
  "getSizedPartitionMap" should "properly index skipped intervals" in {
    val map = Map(0L -> 100L, 1L -> 0L, 2L -> 100L)
    val m = SparkDruidIndexer.getSizedPartitionMap(map, 1000)
    m.size should equal(2)
    m.get((0L, 0L)) should equal(Some(0L))
    m.get((2L, 0L)) should equal(Some(1L))
  }
}

object StaticTestSparkDruidIndexer
{
  val log = new Logger(classOf[TestSparkDruidIndexer])
}
