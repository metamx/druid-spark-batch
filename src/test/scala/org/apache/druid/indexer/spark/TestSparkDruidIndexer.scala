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

import java.io.{Closeable, File}
import java.nio.file.Files
import java.util

import com.google.common.io.Closer
import org.apache.commons.io.FileUtils
import org.apache.druid.data.input.impl._
import org.apache.druid.data.input.parquet.avro.ParquetAvroHadoopInputRowParser
import org.apache.druid.data.input.parquet.simple.ParquetParseSpec
import org.apache.druid.java.util.common.granularity.Granularities
import org.apache.druid.java.util.common.logger.Logger
import org.apache.druid.java.util.common.{IAE, JodaUtils}
import org.apache.druid.query.aggregation.LongSumAggregatorFactory
import org.apache.druid.segment.QueryableIndexIndexableAdapter
import org.apache.druid.segment.column.NumericColumn
import org.apache.druid.utils.CompressionUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, Interval}
import org.scalatest._

import scala.collection.JavaConverters._

class TestSparkDruidIndexer extends FlatSpec with Matchers {

  import TestConfProvider._

  "The spark indexer" should "return proper DataSegments" in {
    val data_files = Seq(
      this.getClass.getResource("/text/lineitem.small.tbl").getPath,
      this.getClass.getResource("/text/empty.tbl").getPath
    )
    val outDir = Files.createTempDirectory("segments").toFile
    val closer = Closer.create()
    try {
      val loadResults = SparkDruidIndexer.loadData(
        data_files,
        new SerializedJson(dataSchema),
        SparkBatchIndexTask.mapToSegmentIntervals(Seq(interval), Granularities.YEAR),
        rowsPerPartition,
        rowsPerFlush,
        outDir.toString,
        indexSpec,
        buildV9Directly,
        buildSparkSession(outDir, closer)
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
        val segDir = Files.createTempDirectory(outDir.toPath, "loadableSegment-%s" format segment.getId).toFile
        val copyResult = CompressionUtils.unzip(file, segDir)
        copyResult.size should be > 0L
        copyResult.getFiles.asScala.map(_.getName).toSet should equal(
          Set("00000.smoosh", "meta.smoosh", "version.bin", "factory.json")
        )
        val index = StaticIndex.INDEX_IO.loadIndex(segDir)
        try {
          val indexAdapter = new QueryableIndexIndexableAdapter(index)
          indexAdapter.getDimensionNames.asScala.toSet should
            equal(
              dataSchema.getParser.getParseSpec.getDimensionsSpec.getDimensionNames.asScala.toSet --
                dataSchema.getParser.getParseSpec.getDimensionsSpec.getDimensionExclusions.asScala.toSet
            )
          for (dimension <- indexAdapter.getDimensionNames.iterator().asScala) {
            val dimVal = indexAdapter.getDimValueLookup[String](dimension).asScala
            dimVal should not be 'Empty
            for (dv <- dimVal) {
              Option(dv) match {
                case Some(_) =>
                  // I had a problem at one point where dimension values were being stored as lists
                  // This is a check to make sure the dimension is a list of values rather than being a list of lists
                  // If the unit test is ever modified to have dimension values that start with this offending case
                  // then of course this test will fail.
                  dv.toString should not startWith "List("
                  dv.toString should not startWith "Set("
                case None => //Ignore
              }
            }
          }
          indexAdapter.getNumRows should be > 0
          for (colName <- Seq("count")) {
            val column = index.getColumnHolder(colName).getColumn.asInstanceOf[NumericColumn]
            try {
              for (i <- Range.apply(0, indexAdapter.getNumRows)) {
                column.getLongSingleValueRow(i) should not be 0
              }
            }
            finally {
              column.close()
            }
          }
          for (colName <- Seq("L_QUANTITY_longSum")) {
            val column = index.getColumnHolder(colName).getColumn.asInstanceOf[NumericColumn]
            try {
              Range.apply(0, indexAdapter.getNumRows).map(column.getLongSingleValueRow).sum should not be 0
            }
            finally {
              column.close()
            }
          }
          /*for (colName <- Seq("L_DISCOUNT_doubleSum", "L_TAX_doubleSum")) {
            val columnHolder = index.getColumnHolder(colName)
            val valueSelector = columnHolder.makeNewSettableColumnValueSelector()
            try {
              Range.apply(0, indexAdapter.getNumRows).map(_ => valueSelector.getDouble).sum should not be 0.0D
            }
            finally {
              columnHolder.getColumn.close()
            }
          }*/
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
    val data_files = Seq(this.getClass.getResource("/text/event.json").toString)
    val closer = Closer.create()
    val outDir = Files.createTempDirectory("segments").toFile
    try {
      val aggName = "agg_met1"
      val aggregatorFactory = new LongSumAggregatorFactory(aggName, "met1")
      val dataSchema = buildDataSchema(
        parseSpec = new JSONParseSpec(
          new TimestampSpec("ts", null, null),
          new DimensionsSpec(
            Seq(new StringDimensionSchema("dim1").asInstanceOf[DimensionSchema]).asJava,
            Seq("ts").asJava,
            null
          ),
          null,
          null
        ),
        aggFactories = Seq(aggregatorFactory)
      )
      val loadResults = SparkDruidIndexer.loadData(
        data_files,
        new SerializedJson(dataSchema),
        SparkBatchIndexTask.mapToSegmentIntervals(Seq(interval), Granularities.YEAR),
        rowsPerPartition,
        rowsPerFlush,
        outDir.toString,
        indexSpec,
        buildV9Directly,
        buildSparkSession(outDir, closer)
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
        val segDir = Files.createTempDirectory(outDir.toPath, "loadableSegment-%s" format segment.getId).toFile
        val copyResult = CompressionUtils.unzip(file, segDir)
        copyResult.size should be > 0L
        copyResult.getFiles.asScala.map(_.getName).toSet should equal(
          Set("00000.smoosh", "meta.smoosh", "version.bin", "factory.json")
        )
        val index = StaticIndex.INDEX_IO.loadIndex(segDir)
        try {
          val qindex = new QueryableIndexIndexableAdapter(index)
          qindex.getDimensionNames.asScala.toSet should equal(Set("dim1"))
          val dimVal : Iterable[String] = qindex.getDimValueLookup("dim1").asScala
          dimVal should not be 'Empty
          for (dv <- dimVal) {
            dv should equal("val1")
          }
          qindex.getMetricNames.asScala.toSet should equal(Set(aggName))
          qindex.getMetricType(aggName) should equal(aggregatorFactory.getTypeName)
          qindex.getNumRows should be(1)
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

  it should "return proper DataSegments from parquet" in {
    val dataFiles = Seq(this.getClass.getResource("/parquet/event.parquet").toString)
    val closer = Closer.create()
    val outDir = Files.createTempDirectory("segments").toFile
    try {
      val aggName = "agg_met1"
      val aggregatorFactory = new LongSumAggregatorFactory(aggName, "met1")
      val dataSchema = buildDataSchema(
        parseSpec = new
          ParquetParseSpec(
            new TimestampSpec("ts", null, null),
            new DimensionsSpec(
              Seq(new StringDimensionSchema("dim1").asInstanceOf[DimensionSchema]).asJava,
              Seq("ts").asJava,
              null
            ),
            null
          ),
        parser = spec => new ParquetAvroHadoopInputRowParser(spec, false),
        aggFactories = Seq(aggregatorFactory)
      )

      val loadResults = SparkDruidIndexer.loadData(
        dataFiles,
        new SerializedJson(dataSchema),
        SparkBatchIndexTask.mapToSegmentIntervals(Seq(interval), Granularities.YEAR),
        rowsPerPartition,
        rowsPerFlush,
        outDir.toString,
        indexSpec,
        buildV9Directly,
        buildSparkSession(outDir, closer)
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
        val segDir = Files.createTempDirectory(outDir.toPath, "loadableSegment-%s" format segment.getId.toString).toFile
        val copyResult = CompressionUtils.unzip(file, segDir)
        copyResult.size should be > 0L
        copyResult.getFiles.asScala.map(_.getName).toSet should equal(
          Set("00000.smoosh", "meta.smoosh", "version.bin", "factory.json")
        )
        val index = StaticIndex.INDEX_IO.loadIndex(segDir)
        try {
          val qIndex = new QueryableIndexIndexableAdapter(index)
          qIndex.getDimensionNames.asScala.toSet should equal(Set("dim1"))
          val dimVal = qIndex.getDimValueLookup[String]("dim1").asScala
          dimVal should not be 'Empty
          for (dv <- dimVal) {
            dv should equal("val1")
          }
          qIndex.getMetricNames.asScala.toSet should equal(Set(aggName))
          qIndex.getMetricType(aggName) should equal(aggregatorFactory.getTypeName)
          qIndex.getNumRows should be(1)
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
    val intervals = SparkBatchIndexTask.mapToSegmentIntervals(Seq(Interval.parse("1992/1993")), Granularities.YEAR)
    val partitioner = new DateBucketPartitioner(Granularities.YEAR, intervals)
    partitioner.getPartition((intervals.head.getStartMillis, 0)) should equal(0L)
  }


  it should "throw an error if out of bounds" in {
    val intervals = SparkBatchIndexTask.mapToSegmentIntervals(Seq(Interval.parse("1992/1993")), Granularities.YEAR)
    val partitioner = new DateBucketPartitioner(Granularities.YEAR, intervals)
    an[IAE] should be thrownBy {
      partitioner.getPartition((0, 0))
    }
  }

  it should "properly partition for multiple timespans" in {
    val intervals = SparkBatchIndexTask.mapToSegmentIntervals(Seq(Interval.parse("1992/1999")), Granularities.YEAR)
    val partitioner = new DateBucketPartitioner(Granularities.YEAR, intervals)
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
    val intervals = SparkBatchIndexTask.mapToSegmentIntervals(Seq(Interval.parse("1992/1993")), Granularities.YEAR)
    val partitioner = new
        DateBucketAndHashPartitioner(Granularities.YEAR, Map((new DateTime("1992").getMillis, 0L) -> 0))
    partitioner.getPartition(makeEvent(intervals.head.getStart)) should equal(0)
  }


  it should "properly partition multiple date ranges" in {
    val intervals = SparkBatchIndexTask.mapToSegmentIntervals(Seq(Interval.parse("1992/1994")), Granularities.YEAR)
    val partitioner = new DateBucketAndHashPartitioner(
      Granularities.YEAR,
      Map((new DateTime("1992").getMillis, 0L) -> 0, (new DateTime("1993").getMillis, 0L) -> 1)
    )
    partitioner.getPartition(makeEvent(intervals.head.getStart)) should equal(0)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L))) should equal(1)
  }


  it should "properly partition disjoint date ranges" in {
    val intervals = SparkBatchIndexTask
      .mapToSegmentIntervals(Seq(Interval.parse("1992/1993"), Interval.parse("1995/1996")), Granularities.YEAR)
    val partitioner = new DateBucketAndHashPartitioner(
      Granularities.YEAR,
      Map((new DateTime("1992").getMillis, 0L) -> 0, (new DateTime("1995").getMillis, 0L) -> 1)
    )
    partitioner.getPartition(makeEvent(intervals.head.getStart)) should equal(0)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L))) should equal(1)
  }

  "The DateBucketAndHashPartitioner workflow" should "properly partition multiple date ranges and buckets" in {
    val intervals = SparkBatchIndexTask.mapToSegmentIntervals(Seq(Interval.parse("1992/1994")), Granularities.YEAR)
    val map = Map(new DateTime("1992").getMillis -> 100L, new DateTime("1993").getMillis -> 200L)
    val m = SparkDruidIndexer.getSizedPartitionMap(map, 150)
    val partitioner = new DateBucketAndHashPartitioner(Granularities.YEAR, m)
    partitioner.getPartition(makeEvent(intervals.head.getStart)) should equal(0)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L))) should equal(1)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L), "something else1")) should equal(2)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L), "anotherHash3")) should equal(2)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L), "anotherHash1")) should equal(1)
  }

  it should "properly partition multiple date ranges and buckets when dim is specified" in {
    val intervals = SparkBatchIndexTask.mapToSegmentIntervals(Seq(Interval.parse("1992/1994")), Granularities.YEAR)
    val map = Map(new DateTime("1992").getMillis -> 100L, new DateTime("1993").getMillis -> 200L)
    val m = SparkDruidIndexer.getSizedPartitionMap(map, 150)
    val partitioner = new DateBucketAndHashPartitioner(Granularities.YEAR, m, Option(Set("dim1")))
    partitioner.getPartition(makeEvent(intervals.head.getStart)) should equal(0)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L))) should equal(1)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L), "something else1")) should equal(2)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L), "anotherHash3")) should equal(2)
    partitioner.getPartition(makeEvent(intervals.last.getEnd.minus(10L), "anotherHash1")) should equal(1)
  }

  it should "properly group multiple events together" in {
    val intervals = SparkBatchIndexTask.mapToSegmentIntervals(Seq(Interval.parse("1992/1994")), Granularities.YEAR)
    val map = Map(new DateTime("1992").getMillis -> 100L, new DateTime("1993").getMillis -> 200L)
    val m = SparkDruidIndexer.getSizedPartitionMap(map, 150)
    val partitioner = new DateBucketAndHashPartitioner(Granularities.YEAR, m, Option(Set[String]()))
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
        DateBucketAndHashPartitioner(Granularities.YEAR, Map[(Long, Long), Int]((0L, 0L) -> 1, (0L, 0L) -> 2))
    partitioner.getPartition(
      100000L -> new util.HashMap[String, AnyRef]() {
        override def hashCode(): Int = {
          Integer.MIN_VALUE
        }
      }
    ) should be(2)
  }

  private def buildSparkSession(outDir: File, closer: Closer): SparkSession = {
    (outDir.mkdirs() || outDir.exists()) && outDir.isDirectory should be(true)
    closer.register(
      new Closeable() {
        override def close(): Unit = FileUtils.deleteDirectory(outDir)
      }
    )

    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[*]")
      .set("user.timezone", "UTC")
      .set("file.encoding", "UTF-8")
      .set("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager")
      .set("org.jboss.logging.provider", "slf4j")
      .set("druid.processing.columnCache.sizeBytes", "1000000000")
      .set("spark.driver.host", "localhost")
      .set("spark.executor.userClassPathFirst", "true")
      .set("spark.driver.userClassPathFirst", "true")
      .set("spark.kryo.referenceTracking", "false")
      .set("spark.kryo.registrator", "org.apache.druid.indexer.spark.SparkDruidRegistrator")
      .registerKryoClasses(SparkBatchIndexTask.getKryoClasses())

    val sparkSession = SparkSession.builder.config(conf).getOrCreate()
    closer.register(
      new Closeable {
        override def close(): Unit = sparkSession.stop()
      }
    )
    sparkSession
  }
}

object StaticTestSparkDruidIndexer {
  val log = new Logger(classOf[TestSparkDruidIndexer])
}
