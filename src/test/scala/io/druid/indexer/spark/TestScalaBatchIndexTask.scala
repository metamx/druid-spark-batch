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

import java.util.Properties

import com.fasterxml.jackson.databind.module.SimpleModule
import io.druid.data.input.impl.{DelimitedParseSpec, DimensionsSpec, TimestampSpec}
import io.druid.jackson.DefaultObjectMapper
import io.druid.query.aggregation.{CountAggregatorFactory, DoubleSumAggregatorFactory, LongSumAggregatorFactory}
import org.joda.time.Interval
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConversions._

class TestScalaBatchIndexTask extends FlatSpec with Matchers
{
  val objectMapper           = new DefaultObjectMapper()
    .registerModule(new SimpleModule("TestScalaBatchIndexTask").registerSubtypes(ScalaBatchIndexTask.getClass))
  val taskId                 = "taskId"
  val dataSource             = "defaultDataSource"
  val interval               = Interval.parse("2010/2020")
  val dataFile               = "file:/someFile"
  val parseSpec              = new DelimitedParseSpec(
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
  val outPath                = "file:/tmp/foo"
  val rowsPerPartition: Long = 8139L
  val rowsPerFlush    : Int  = 389
  val aggFactories           = Seq(
    new CountAggregatorFactory("count"),
    new LongSumAggregatorFactory("L_QUANTITY_longSum", "l_quantity"),
    new DoubleSumAggregatorFactory("L_EXTENDEDPRICE_doubleSum", "l_extendedprice"),
    new DoubleSumAggregatorFactory("L_DISCOUNT_doubleSum", "l_discount"),
    new DoubleSumAggregatorFactory("L_TAX_doubleSum", "l_tax")
  )
  val properties             = Seq(
    ("user.timezone", "UTC"),
    ("file.encoding", "UTF-8"),
    ("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager"),
    ("org.jboss.logging.provider", "log4j2"),
    ("druid.processing.columnCache.sizeBytes", "1000000000"),
    ("some.property", "someValue")
  ).foldLeft(new Properties())(
      (p, e) => {
        p.setProperty(e._1, e._2)
        p
      }
    )
  val master                 = "local[999]"

  "The ScalaBatchIndexTask" should "properly SerDe a full object" in {

    val taskPre = new ScalaBatchIndexTask(
      taskId,
      dataSource,
      interval,
      dataFile,
      parseSpec,
      outPath,
      aggFactories,
      rowsPerPartition,
      rowsPerFlush,
      properties,
      master
    )
    val taskPost = objectMapper.readValue(objectMapper.writeValueAsString(taskPre), classOf[ScalaBatchIndexTask])
    assert(taskPre.equals(taskPost))
  }

  "The ScalaBatchIndexTask" should "be equal for equal tasks" in {
    val task1 = new ScalaBatchIndexTask(
      taskId,
      dataSource,
      interval,
      dataFile,
      parseSpec,
      outPath,
      aggFactories,
      rowsPerPartition,
      rowsPerFlush,
      properties,
      master
    )
    val task2 = new ScalaBatchIndexTask(
      taskId,
      dataSource,
      interval,
      dataFile,
      parseSpec,
      outPath,
      aggFactories,
      rowsPerPartition,
      rowsPerFlush,
      properties,
      master
    )
    assert(task1.equals(task2))
    assert(task2.equals(task1))
  }

  "The ScalaBatchIndexTask" should "not be equal for dissimilar tasks" in {
    val task1 = new ScalaBatchIndexTask(
      taskId,
      dataSource,
      interval,
      dataFile,
      parseSpec,
      outPath,
      aggFactories,
      rowsPerPartition,
      rowsPerFlush,
      properties,
      master
    )
    assert(
      task1.equals(
        new ScalaBatchIndexTask(
          taskId,
          dataSource,
          interval,
          dataFile,
          parseSpec,
          outPath,
          aggFactories,
          rowsPerPartition,
          rowsPerFlush,
          properties,
          master
        )
      )
    )
    assert(
      !task1.equals(
        new ScalaBatchIndexTask(
          taskId + "something else",
          dataSource,
          interval,
          dataFile,
          parseSpec,
          outPath,
          aggFactories,
          rowsPerPartition,
          rowsPerFlush,
          properties,
          master
        )
      )
    )

    assert(
      !task1.equals(
        new ScalaBatchIndexTask(
          taskId,
          dataSource + "something else",
          interval,
          dataFile,
          parseSpec,
          outPath,
          aggFactories,
          rowsPerPartition,
          rowsPerFlush,
          properties,
          master
        )
      )
    )

    assert(
      !task1.equals(
        new ScalaBatchIndexTask(
          taskId,
          dataSource,
          interval,
          dataFile + "something else",
          parseSpec,
          outPath,
          aggFactories,
          rowsPerPartition,
          rowsPerFlush,
          properties,
          master
        )
      )
    )

    assert(
      !task1.equals(
        new ScalaBatchIndexTask(
          taskId,
          dataSource,
          interval,
          dataFile,
          parseSpec,
          outPath,
          aggFactories,
          rowsPerPartition + 1,
          rowsPerFlush,
          properties,
          master
        )
      )
    )


    assert(
      !task1.equals(
        new ScalaBatchIndexTask(
          taskId,
          dataSource,
          interval,
          dataFile,
          parseSpec,
          outPath,
          aggFactories,
          rowsPerPartition,
          rowsPerFlush + 1,
          properties,
          master
        )
      )
    )


    assert(
      !task1.equals(
        new ScalaBatchIndexTask(
          taskId,
          dataSource,
          interval,
          dataFile,
          parseSpec,
          outPath,
          aggFactories,
          rowsPerPartition,
          rowsPerFlush,
          properties,
          master + "something else"
        )
      )
    )

    assert(
      !task1.equals(
        new ScalaBatchIndexTask(
          taskId,
          dataSource,
          interval,
          dataFile,
          parseSpec,
          outPath,
          aggFactories ++ List(new CountAggregatorFactory("foo")),
          rowsPerPartition,
          rowsPerFlush,
          properties,
          master
        )
      )
    )

    val notSameProperties = new Properties(properties)
    notSameProperties.setProperty("Something not present", "some value")
    assert(
      !task1.equals(
        new ScalaBatchIndexTask(
          taskId,
          dataSource,
          interval,
          dataFile,
          parseSpec,
          outPath,
          aggFactories,
          rowsPerPartition,
          rowsPerFlush,
          notSameProperties,
          master
        )
      )
    )
  }
}
