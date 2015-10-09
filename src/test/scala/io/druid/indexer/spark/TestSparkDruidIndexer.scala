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
import java.util

import com.google.common.io.Closer
import com.metamx.common.logger.Logger
import io.druid.data.input.impl.{DelimitedParseSpec, DimensionsSpec, ParseSpec, TimestampSpec}
import io.druid.granularity.QueryGranularity
import io.druid.query.aggregation.{CountAggregatorFactory, DoubleSumAggregatorFactory, LongSumAggregatorFactory}
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.Interval
import org.scalatest._

import scala.collection.JavaConversions._

class TestSparkDruidIndexer extends FlatSpec with Matchers
{
  "The spark indexer" should "return proper DataSegments" in {
    val data_file = this.getClass.getResource("/lineitem.small.tbl").toString
    val closer = Closer.create()
    val outDir = Files.createTempDirectory("segments").toFile
    assert((outDir.mkdirs() || outDir.exists()) && outDir.isDirectory)
    closer.register(
      new Closeable()
      {
        override def close(): Unit = FileUtils.deleteDirectory(outDir)
      }
    )
    try {
      val conf = new SparkConf()
        .setAppName("Simple Application")
        .setMaster("local[1]")
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
      val dataSource = "dataSource"

      val parseSpec = new SerializedJson[ParseSpec](
        new DelimitedParseSpec(
          new TimestampSpec("l_shipdate", "yyyy-MM-dd", null),
          new DimensionsSpec(
            seqAsJavaList(
              Seq(
                "l_orderkey",
                "l_suppkey",
                "l_linenumber",
                "l_returnflag",
                "l_linestatus",
                "l_commitdate",
                "l_receiptdate",
                "l_shipinstruct",
                "l_shipmode",
                "l_comment"
              )
            ),
            seqAsJavaList(
              Seq(
                "l_shipdate",
                "l_tax",
                "count",
                "l_quantity",
                "l_discount",
                "l_extendedprice"
              )
            ),
            null
          ),
          "|",
          ",",
          seqAsJavaList(
            Seq(
              "l_orderkey",
              "l_partkey",
              "l_suppkey",
              "l_linenumber",
              "l_quantity",
              "l_extendedprice",
              "l_discount",
              "l_tax",
              "l_returnflag",
              "l_linestatus",
              "l_shipdate",
              "l_commitdate",
              "l_receiptdate",
              "l_shipinstruct",
              "l_shipmode",
              "l_comment"
            )
          )
        )
      )
      val aggFactories = Seq(
        new CountAggregatorFactory("count"),
        new LongSumAggregatorFactory("L_QUANTITY_longSum", "l_quantity"),
        new DoubleSumAggregatorFactory("L_EXTENDEDPRICE_doubleSum", "l_extendedprice"),
        new DoubleSumAggregatorFactory("L_DISCOUNT_doubleSum", "l_discount"),
        new DoubleSumAggregatorFactory("L_TAX_doubleSum", "l_tax")
      )
      val sc = new SparkContext(conf)
      closer.register(
        new Closeable
        {
          override def close(): Unit = sc.stop()
        }
      )
      val loadResults = SparkDruidIndexer.loadData(
        Seq(data_file),
        dataSource,
        parseSpec,
        Interval.parse("1990/2000"),
        aggFactories,
        500L,
        80000,
        outDir.toString,
        QueryGranularity.DAY,
        sc
      )
      assert(loadResults.length == 3)
      val dataSegment = loadResults.head
      assert(dataSegment.getBinaryVersion == 9)
      assert(dataSegment.getDataSource.equals("dataSource"))
    }
    finally {
      closer.close()
    }
  }
}

object StaticTestSparkDruidIndexer
{
  val log = new Logger(classOf[TestSparkDruidIndexer])
}
