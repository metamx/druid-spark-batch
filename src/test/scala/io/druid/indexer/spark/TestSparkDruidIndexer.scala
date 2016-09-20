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

import com.google.common.collect.ImmutableList
import com.google.common.io.Closer
import com.metamx.common.logger.Logger
import com.metamx.common.{CompressionUtils, Granularity, IAE}
import io.druid.common.utils.JodaUtils
import io.druid.data.input.impl.{DimensionsSpec, JSONParseSpec, StringDimensionSchema, TimestampSpec}
import io.druid.query.aggregation.LongSumAggregatorFactory
import io.druid.segment.QueryableIndexIndexableAdapter
import java.io.{Closeable, File}
import java.nio.file.Files
import java.util
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Interval}
import org.scalatest._
import scala.collection.JavaConverters._


class TestSparkDruidIndexer extends FlatSpec with Matchers
{

  import TestScalaBatchIndexTask._

  "The spark indexer" should "return proper DataSegments" in {
    val data_files = Seq(
      this.getClass.getResource("/lineitem.small.tbl").toString,
      this.getClass.getResource("/empty.tbl").toString
    )
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
        .registerKryoClasses(SparkBatchIndexTask.getKryoClasses())

      val sc = new SparkContext(conf)
      closer.register(
        new Closeable
        {
          override def close(): Unit = sc.stop()
        }
      )

      val loadResults = SparkDruidIndexer.loadData(
        data_files,
        new SerializedJson(dataSchema),
        SparkBatchIndexTask.mapToSegmentIntervals(Seq(interval), Granularity.YEAR),
        rowsPerPartition,
        rowsPerFlush,
        outDir.toString,
        indexSpec,
        sc
      )
      loadResults.length should be(7)
      for (
        segment <- loadResults
      ) {
        segment.getBinaryVersion should be(9)
        segment.getDataSource should equal(dataSource)
        interval.contains(segment.getInterval) should be(true)
        segment.getInterval.contains(interval) should be(false)
        segment.getSize should be > 0L
        segment.getDimensions.asScala.toSet should
          equal(
            dataSchema.getParser.getParseSpec.getDimensionsSpec.getDimensionNames.asScala.toSet --
              dataSchema.getParser.getParseSpec.getDimensionsSpec.getDimensionExclusions.asScala.toSet
          )
        segment.getMetrics.asScala.toList should equal(dataSchema.getAggregators.map(_.getName).toList)
        val file = new File(segment.getLoadSpec.get("path").toString)
        file.exists() should be(true)
        val segDir = Files.createTempDirectory(outDir.toPath, "loadableSegment-%s" format segment.getIdentifier).toFile
        val copyResult = CompressionUtils.unzip(file, segDir)
        copyResult.size should be > 0L
        copyResult.getFiles.asScala.map(_.getName).toSet should equal(Set("00000.smoosh", "meta.smoosh", "version.bin"))
        val index = StaticIndex.INDEX_IO.loadIndex(segDir)
        try {
          val qindex = new QueryableIndexIndexableAdapter(index)
          qindex.getDimensionNames.asScala.toSet should
            equal(
              dataSchema.getParser.getParseSpec.getDimensionsSpec.getDimensionNames.asScala.toSet --
                dataSchema.getParser.getParseSpec.getDimensionsSpec.getDimensionExclusions.asScala.toSet
            )
          for (dimension <- qindex.getDimensionNames.iterator().asScala) {
            val dimVal = qindex.getDimValueLookup(dimension).asScala
            dimVal should not be 'Empty
            for (dv <- dimVal) {
              Option(dv) match {
                case Some(v) =>
                  dv should not be null
                  // I had a problem at one point where dimension values were being stored as lists
                  // This is a check to make sure the dimension is a list of values rather than being a list of lists
                  // If the unit test is ever modified to have dimension values that start with this offending case
                  // then of course this test will fail.
                  dv should not startWith "List("
                  dv should not startWith "Set("
                case None => //Ignore
              }
            }
          }
          qindex.getNumRows should be > 0
          for (colName <- Seq("count")) {
            val column = index.getColumn(colName).getGenericColumn
            try {
              for (i <- Range.apply(0, qindex.getNumRows)) {
                column.getLongSingleValueRow(i) should not be 0
              }
            }
            finally {
              column.close()
            }
          }
          for (colName <- Seq("L_QUANTITY_longSum")) {
            val column = index.getColumn(colName).getGenericColumn
            try {
              Range.apply(0, qindex.getNumRows).map(column.getLongSingleValueRow).sum should not be 0
            }
            finally {
              column.close()
            }
          }
          for (colName <- Seq("L_DISCOUNT_doubleSum", "L_TAX_doubleSum")) {
            val column = index.getColumn(colName).getGenericColumn
            try {
              Range.apply(0, qindex.getNumRows).map(column.getFloatSingleValueRow).sum should not be 0.0D
            }
            finally {
              column.close()
            }
          }
          index.getDataInterval.getEnd.getMillis should not be JodaUtils.MAX_INSTANT
        }
        finally {
          index.close()
        }
      }
    }
    finally {
      closer.close()
    }
  }


  it should "return proper DataSegments from json" in {
    val data_files = Seq(this.getClass.getResource("/event.json").toString)
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
        .registerKryoClasses(SparkBatchIndexTask.getKryoClasses())

      val sc = new SparkContext(conf)
      closer.register(
        new Closeable
        {
          override def close(): Unit = sc.stop()
        }
      )
      val aggName = "agg_met1"
      val aggregatorFactory = new LongSumAggregatorFactory(aggName, "met1")
      val dataSchema = buildDataSchema(
        parseSpec = new
            JSONParseSpec(
              new TimestampSpec("ts", null, null),
              new DimensionsSpec(ImmutableList.of(new StringDimensionSchema("dim1")), ImmutableList.of("ts"), null),
              null,
              null
            ),
        aggFactories = Seq(aggregatorFactory)
      )

      val loadResults = SparkDruidIndexer.loadData(
        data_files,
        new SerializedJson(dataSchema),
        SparkBatchIndexTask.mapToSegmentIntervals(Seq(interval), Granularity.YEAR),
        rowsPerPartition,
        rowsPerFlush,
        outDir.toString,
        indexSpec,
        sc
      )
      loadResults.length should be(1)
      for (
        segment <- loadResults
      ) {
        segment.getBinaryVersion should be(9)
        segment.getDataSource should equal(dataSource)
        interval.contains(segment.getInterval) should be(true)
        segment.getInterval.contains(interval) should be(false)
        segment.getSize should be > 0L
        segment.getDimensions.asScala.toSet should equal(Set("dim1"))
        segment.getMetrics.asScala.toList should equal(dataSchema.getAggregators.map(_.getName).toList)
        val file = new File(segment.getLoadSpec.get("path").toString)
        file.exists() should be(true)
        val segDir = Files.createTempDirectory(outDir.toPath, "loadableSegment-%s" format segment.getIdentifier).toFile
        val copyResult = CompressionUtils.unzip(file, segDir)
        copyResult.size should be > 0L
        copyResult.getFiles.asScala.map(_.getName).toSet should equal(Set("00000.smoosh", "meta.smoosh", "version.bin"))
        val index = StaticIndex.INDEX_IO.loadIndex(segDir)
        try {
          val qindex = new QueryableIndexIndexableAdapter(index)
          qindex.getDimensionNames.asScala.toSet should equal(Set("dim1"))
          val dimVal = qindex.getDimValueLookup("dim1").asScala
          dimVal should not be 'Empty
          for (dv <- dimVal) {
            dv should equal("val1")
          }
          qindex.getMetricNames.asScala.toSet should equal(Set(aggName))
          qindex.getMetricType(aggName) should equal(aggregatorFactory.getTypeName)
          qindex.getNumRows should be(1)
          qindex.getRows.asScala.head.getMetrics()(0) should be(1)
          index.getDataInterval.getEnd.getMillis should not be JodaUtils.MAX_INSTANT
        }
        finally {
          index.close()
        }
      }
    }
    finally {
      closer.close()
    }
  }

  "The DateBucketPartitioner" should "properly partition single item data" in {
    val intervals = SparkBatchIndexTask.mapToSegmentIntervals(Seq(Interval.parse("1992/1993")), Granularity.YEAR)
    val partitioner = new DateBucketPartitioner(Granularity.YEAR, intervals)
    partitioner.getPartition((intervals.head.getStartMillis, 0)) should equal(0L)
  }


  it should "throw an error if out of bounds" in {
    val intervals = SparkBatchIndexTask.mapToSegmentIntervals(Seq(Interval.parse("1992/1993")), Granularity.YEAR)
    val partitioner = new DateBucketPartitioner(Granularity.YEAR, intervals)
    an[IAE] should be thrownBy {
      partitioner.getPartition((0, 0))
    }
  }

  it should "properly partition for multiple timespans" in {
    val intervals = SparkBatchIndexTask.mapToSegmentIntervals(Seq(Interval.parse("1992/1999")), Granularity.YEAR)
    val partitioner = new DateBucketPartitioner(Granularity.YEAR, intervals)
    var idex = 0
    intervals.foreach(
      i => {
        partitioner.getPartition((i.getStartMillis, 0)) should equal(idex)
        idex += 1
      }
    )
    idex should be(intervals.size)
  }

  it should "properly partition single data" in {
    val intervals = SparkBatchIndexTask.mapToSegmentIntervals(Seq(Interval.parse("1992/1993")), Granularity.YEAR)
    val partitioner = new
        DateBucketAndHashPartitioner(Granularity.YEAR, Map((new DateTime("1992").getMillis, 0L) -> 0))
    partitioner.getPartition(makeEvent(intervals.head.getStart)) should equal(0)
  }


  it should "properly partition multiple date ranges" in {
    val intervals = SparkBatchIndexTask.mapToSegmentIntervals(Seq(Interval.parse("1992/1994")), Granularity.YEAR)
    val partitioner = new DateBucketAndHashPartitioner(
      Granularity.YEAR,
      Map((new DateTime("1992").getMillis, 0L) -> 0, (new DateTime("1993").getMillis, 0L) -> 1)
    )
    partitioner.getPartition(makeEvent(intervals.head.getStart)) should equal(0)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L))) should equal(1)
  }


  it should "properly partition disjoint date ranges" in {
    val intervals = SparkBatchIndexTask
      .mapToSegmentIntervals(Seq(Interval.parse("1992/1993"), Interval.parse("1995/1996")), Granularity.YEAR)
    val partitioner = new DateBucketAndHashPartitioner(
      Granularity.YEAR,
      Map((new DateTime("1992").getMillis, 0L) -> 0, (new DateTime("1995").getMillis, 0L) -> 1)
    )
    partitioner.getPartition(makeEvent(intervals.head.getStart)) should equal(0)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L))) should equal(1)
  }

  "The DateBucketAndHashPartitioner workflow" should "properly partition multiple date ranges and buckets" in {
    val intervals = SparkBatchIndexTask.mapToSegmentIntervals(Seq(Interval.parse("1992/1994")), Granularity.YEAR)
    val map = Map(new DateTime("1992").getMillis -> 100L, new DateTime("1993").getMillis -> 200L)
    val m = SparkDruidIndexer.getSizedPartitionMap(map, 150)
    val partitioner = new DateBucketAndHashPartitioner(Granularity.YEAR, m)
    partitioner.getPartition(makeEvent(intervals.head.getStart)) should equal(0)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L))) should equal(1)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L), "something else1")) should equal(2)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L), "anotherHash3")) should equal(2)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L), "anotherHash1")) should equal(1)
  }

  it should "properly partition multiple date ranges and buckets when dim is specified" in
    {
      val intervals = SparkBatchIndexTask.mapToSegmentIntervals(Seq(Interval.parse("1992/1994")), Granularity.YEAR)
      val map = Map(new DateTime("1992").getMillis -> 100L, new DateTime("1993").getMillis -> 200L)
      val m = SparkDruidIndexer.getSizedPartitionMap(map, 150)
      val partitioner = new DateBucketAndHashPartitioner(Granularity.YEAR, m, Option(Set("dim1")))
      partitioner.getPartition(makeEvent(intervals.head.getStart)) should equal(0)
      partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L))) should equal(1)
      partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L), "something else1")) should equal(2)
      partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L), "anotherHash3")) should equal(2)
      partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L), "anotherHash1")) should equal(1)
    }

  it should "properly group multiple events together" in {
    val intervals = SparkBatchIndexTask.mapToSegmentIntervals(Seq(Interval.parse("1992/1994")), Granularity.YEAR)
    val map = Map(new DateTime("1992").getMillis -> 100L, new DateTime("1993").getMillis -> 200L)
    val m = SparkDruidIndexer.getSizedPartitionMap(map, 150)
    val partitioner = new DateBucketAndHashPartitioner(Granularity.YEAR, m, Option(Set[String]()))
    partitioner.getPartition(makeEvent(intervals.head.getStart)) should equal(0)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L))) should equal(1)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L), "something else1")) should equal(1)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L), "anotherHash3")) should equal(1)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L), "anotherHash1")) should equal(1)
  }

  def makeEvent(d: DateTime, v: String = "dim11"): (Long, Map[String, List[String]]) = {
    d.getMillis -> Map("dim1" -> List(v))
  }

  "getSizedPartitionMap" should "partition data correctly for single items" in {
    val map = Map(0L -> 100L)
    val m = SparkDruidIndexer.getSizedPartitionMap(map, 1000)
    m.size should equal(1)
    m.get((0L, 0L)) should equal(Some(0L))
  }

  it should "return nothing if unknown coordinates" in {
    val map = Map(0L -> 100L)
    val m = SparkDruidIndexer.getSizedPartitionMap(map, 1000)
    m.size should equal(1)
    m.get((1L, 0L)) should be('empty)
  }

  it should "partition data correctly for single boundary counts" in {
    val map = Map(0L -> 100L)
    val m = SparkDruidIndexer.getSizedPartitionMap(map, 100)
    m.size should equal(2)
    m.get((0L, 0L)) should equal(Some(0L))
    m.get((0L, 1L)) should equal(Some(1L))
  }

  it should "partition data correctly for multiple date buckets" in {
    val map = Map(0L -> 100L, 1L -> 100L)
    val m = SparkDruidIndexer.getSizedPartitionMap(map, 1000)
    m.size should equal(2)
    m.get((0L, 0L)) should equal(Some(0L))
    m.get((1L, 0L)) should equal(Some(1L))
  }

  it should "ignore empty intervals" in {
    val map = Map(0L -> 100L, 1L -> 0L)
    val m = SparkDruidIndexer.getSizedPartitionMap(map, 1000)
    m.size should equal(1)
    m.get((0L, 0L)) should equal(Some(0L))
  }
  it should "properly index skipped intervals" in {
    val map = Map(0L -> 100L, 1L -> 0L, 2L -> 100L)
    val m = SparkDruidIndexer.getSizedPartitionMap(map, 1000)
    m.size should equal(2)
    m.get((0L, 0L)) should equal(Some(0L))
    m.get((2L, 0L)) should equal(Some(1L))
  }
  "DateBucketAndHashPartitioner" should "handle min value hash" in {
    val partitioner = new
        DateBucketAndHashPartitioner(Granularity.YEAR, Map[(Long, Long), Int]((0L, 0L) -> 1, (0L, 0L) -> 2))
    partitioner.getPartition(
      100000L -> new util.HashMap[String, AnyRef]() {
        override def hashCode(): Int = {
          Integer.MIN_VALUE
        }
      }
    ) should be(2)
  }

  "SparkBatchIndexer version" should "properly parse" in {
    val validTypes = Set("index_spark_2.10", "index_spark_2.11", "index_spark_2.12")
    validTypes should contain (SparkBatchIndexTask.TASK_TYPE)
  }
}

object StaticTestSparkDruidIndexer
{
  val log = new Logger(classOf[TestSparkDruidIndexer])
}
