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

import java.net.URL
import java.util
import java.util.{Collections, Properties}

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.inject.name.Names
import com.google.inject.{Binder, Module}
import org.apache.druid.data.input.impl._
import org.apache.druid.guice.GuiceInjectors
import org.apache.druid.initialization.Initialization
import org.apache.druid.java.util.common.granularity.Granularities
import org.apache.druid.query.aggregation.{AggregatorFactory, CountAggregatorFactory, DoubleSumAggregatorFactory, LongSumAggregatorFactory}
import org.apache.druid.segment.IndexSpec
import org.apache.druid.segment.indexing.DataSchema
import org.apache.druid.segment.indexing.granularity.{GranularitySpec, UniformGranularitySpec}
import org.joda.time.Interval

import scala.collection.JavaConverters._

object TestConfProvider {
  System.setProperty("user.timezone", "UTC")
  val taskConfURL: URL = getClass.getResource(s"/text/${SparkBatchIndexTask.TASK_TYPE_BASE}_spec.json")

  val injector = Initialization
    .makeInjectorWithModules(
      GuiceInjectors.makeStartupInjector(), Collections.singletonList[Module](
        new Module {
          override def configure(binder: Binder): Unit = {
            binder.bindConstant.annotatedWith(Names.named("serviceName")).to("druid/test")
            binder.bindConstant.annotatedWith(Names.named("servicePort")).to(0)
            binder.bindConstant.annotatedWith(Names.named("tlsServicePort")).to(-1)
          }
        }
      )
    )
  val objectMapper = injector.getInstance(classOf[ObjectMapper])
  val taskId = "taskId"
  val dataSource = "defaultDataSource"
  val interval = Interval.parse("1992/1999")
  val dataFiles = Seq("file:/someFile")
  val parseSpec = new DelimitedParseSpec(
    new TimestampSpec("l_shipdate", "yyyy-MM-dd", null),
    new DimensionsSpec(
      Seq(
        "l_orderkey",
        "l_partkey",
        "l_suppkey",
        "l_linenumber",
        "l_returnflag",
        "l_linestatus",
        "l_shipinstruct",
        "l_shipmode",
        "l_comment"
      )
        .map(new StringDimensionSchema(_).asInstanceOf[DimensionSchema])
        .asJava,
      Seq(
        "l_shipdate",
        "l_tax",
        "count",
        "l_quantity",
        "l_discount",
        "l_extendedprice",
        "l_commitdate",
        "l_receiptdate"
      ).asJava,
      null),
    "|",
    ",",
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
    ).asJava,
    false,
    0
  )
  val outPath = "file:/tmp/foo"
  val rowsPerPartition: Long = 8139L
  val rowsPerFlush: Int = 389
  val aggFactories: Seq[AggregatorFactory] = Seq(
    new CountAggregatorFactory("count"),
    new LongSumAggregatorFactory("L_QUANTITY_longSum", "l_quantity"),
    new DoubleSumAggregatorFactory("L_EXTENDEDPRICE_doubleSum", "l_extendedprice"),
    new DoubleSumAggregatorFactory("L_DISCOUNT_doubleSum", "l_discount"),
    new DoubleSumAggregatorFactory("L_TAX_doubleSum", "l_tax")
  )
  val properties = {
    val prop = new Properties()
    prop.putAll(
      Map(
        ("user.timezone", "UTC"),
        ("file.encoding", "UTF-8"),
        ("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager"),
        ("org.jboss.logging.provider", "log4j2"),
        ("druid.processing.columnCache.sizeBytes", "1000000000"),
        ("some.property", "someValue")
      ).asJava
    )
    prop
  }
  val master = "local[*]"

  val granSpec = new UniformGranularitySpec(Granularities.YEAR, Granularities.DAY, Seq(interval).asJava)
  val dataSchema: DataSchema = buildDataSchema()
  val indexSpec = new IndexSpec()
  val classpathPrefix = "somePrefix.jar"
  val hadoopDependencyCoordinates: util.List[String] = Collections.singletonList("some:coordinate:version")
  val buildV9Directly = true

  def buildDataSchema(
      dataSource: String = dataSource,
      parseSpec: ParseSpec = parseSpec,
      aggFactories: Seq[AggregatorFactory] = aggFactories,
      granSpec: GranularitySpec = granSpec,
      mapper: ObjectMapper = objectMapper,
      parser: ParseSpec => InputRowParser[_] = spec => new StringInputRowParser(spec, null)
  ) = new DataSchema(
    dataSource,
    objectMapper.convertValue(parser(parseSpec), new TypeReference[java.util.Map[String, Any]]() {}),
    aggFactories.toArray,
    granSpec,
    null,
    mapper
  )

  def buildSparkBatchIndexTask(
      id: String = taskId,
      dataSchema: DataSchema = dataSchema,
      interval: Interval = interval,
      dataFiles: Seq[String] = dataFiles,
      rowsPerPartition: Long = rowsPerPartition,
      rowsPerPersist: Int = rowsPerFlush,
      properties: Properties = properties,
      master: String = master,
      context: Map[String, Object] = Map(),
      indexSpec: IndexSpec = indexSpec,
      classpathPrefix: String = classpathPrefix,
      hadoopDependencyCoordinates: java.util.List[String] = hadoopDependencyCoordinates,
      buildV9Directly: Boolean = buildV9Directly
  ): SparkBatchIndexTask = new SparkBatchIndexTask(
    id,
    dataSchema,
    Seq(interval).asJava,
    dataFiles.asJava,
    rowsPerPartition,
    rowsPerPersist,
    properties,
    master,
    context.asJava,
    indexSpec,
    classpathPrefix,
    hadoopDependencyCoordinates,
    buildV9Directly
  )

  def buildSparkBatchIndexTaskWithoutV9(
      id: String = taskId,
      dataSchema: DataSchema = dataSchema,
      interval: Interval = interval,
      dataFiles: Seq[String] = dataFiles
  ): SparkBatchIndexTask = new SparkBatchIndexTask(
    id,
    dataSchema,
    Seq(interval).asJava,
    dataFiles.asJava
  )
}
