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

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.inject.Binder
import com.google.inject.Module
import com.google.inject.name.Names
import com.metamx.common.Granularity
import io.druid.data.input.impl._
import io.druid.granularity.QueryGranularities
import io.druid.granularity.QueryGranularity
import io.druid.guice.GuiceInjectors
import io.druid.indexing.common.task.Task
import io.druid.initialization.Initialization
import io.druid.query.aggregation.AggregatorFactory
import io.druid.query.aggregation.CountAggregatorFactory
import io.druid.query.aggregation.DoubleSumAggregatorFactory
import io.druid.query.aggregation.LongSumAggregatorFactory
import io.druid.segment.IndexSpec
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy
import io.druid.segment.data.RoaringBitmapSerdeFactory
import io.druid.segment.indexing.DataSchema
import io.druid.segment.indexing.granularity.GranularitySpec
import io.druid.segment.indexing.granularity.UniformGranularitySpec
import java.util.Collections
import java.util.Properties
import org.joda.time.Interval
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object TestScalaBatchIndexTask
{
  val injector                                 = Initialization
    .makeInjectorWithModules(
      GuiceInjectors.makeStartupInjector(), List[Module](
        new Module
        {
          override def configure(binder: Binder): Unit = {
            binder.bindConstant.annotatedWith(Names.named("serviceName")).to("druid/test")
            binder.bindConstant.annotatedWith(Names.named("servicePort")).to(0)
          }
        }
      )
    )
  val objectMapper                             = injector.getInstance(classOf[ObjectMapper])
  val taskId                                   = "taskId"
  val dataSource                               = "defaultDataSource"
  val interval                                 = Interval.parse("1992/1999")
  val dataFiles                                = Seq("file:/someFile")
  val parseSpec                                = new DelimitedParseSpec(
    new TimestampSpec("l_shipdate", "yyyy-MM-dd", null),
    new DimensionsSpec(
      seqAsJavaList(
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
        ).map(new StringDimensionSchema(_))
      ),
      seqAsJavaList(
        Seq(
          "l_shipdate",
          "l_tax",
          "count",
          "l_quantity",
          "l_discount",
          "l_extendedprice",
          "l_commitdate",
          "l_receiptdate"
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
  val outPath                                  = "file:/tmp/foo"
  val rowsPerPartition: Long                   = 8139L
  val rowsPerFlush    : Int                    = 389
  val aggFactories    : Seq[AggregatorFactory] = Seq(
    new CountAggregatorFactory("count"),
    new LongSumAggregatorFactory("L_QUANTITY_longSum", "l_quantity"),
    new DoubleSumAggregatorFactory("L_EXTENDEDPRICE_doubleSum", "l_extendedprice"),
    new DoubleSumAggregatorFactory("L_DISCOUNT_doubleSum", "l_discount"),
    new DoubleSumAggregatorFactory("L_TAX_doubleSum", "l_tax")
  )
  val properties                               = {
    val prop = new Properties()
    prop.putAll(
      Map(
        ("user.timezone", "UTC"),
        ("file.encoding", "UTF-8"),
        ("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager"),
        ("org.jboss.logging.provider", "log4j2"),
        ("druid.processing.columnCache.sizeBytes", "1000000000"),
        ("some.property", "someValue")
      )
    )
    prop
  }
  val master                                   = "local[999]"

  val granSpec                    = new UniformGranularitySpec(Granularity.YEAR, QueryGranularities.DAY, Seq(interval))
  val dataSchema                  = buildDataSchema()
  val indexSpec                   = new IndexSpec()
  val classpathPrefix             = "somePrefix.jar"
  val hadoopDependencyCoordinates = Collections.singletonList("some:coordinate:version")
  val buildV9Directly             = true

  def buildDataSchema(
    dataSource: String = dataSource,
    parseSpec: ParseSpec = parseSpec,
    aggFactories: Seq[AggregatorFactory] = aggFactories,
    granSpec: GranularitySpec = granSpec,
    mapper: ObjectMapper = objectMapper
  ) = new DataSchema(
    dataSource,
    objectMapper
      .convertValue(new StringInputRowParser(parseSpec, null), new TypeReference[java.util.Map[String, Any]]() {}),
    aggFactories.toArray,
    granSpec,
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
    Seq(interval),
    dataFiles,
    rowsPerPartition,
    rowsPerPersist,
    properties,
    master,
    context,
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
    Seq(interval),
    dataFiles
  )
}

class TestScalaBatchIndexTask extends FlatSpec with Matchers
{

  import TestScalaBatchIndexTask._

  "The ScalaBatchIndexTask" should "properly SerDe a full object" in {
    val taskPre = buildSparkBatchIndexTask()
    val taskPost = objectMapper.readValue(objectMapper.writeValueAsString(taskPre), classOf[SparkBatchIndexTask])
    val implVersion = classOf[QueryGranularity].getPackage.getImplementationVersion
    taskPre.rowFlushBoundary_ should equal(taskPost.rowFlushBoundary_)
    taskPre.getDataSchema.getAggregators should equal(taskPost.getDataSchema.getAggregators)
    taskPre.getDataSchema.getDataSource should equal(taskPost.getDataSchema.getDataSource)
    taskPre.getDataSchema.getGranularitySpec should equal(taskPost.getDataSchema.getGranularitySpec)
    // Skip checking parser.. see https://github.com/druid-io/druid/issues/2914
    taskPre.getDataFiles should equal(taskPost.getDataFiles)
    taskPre.aggregatorFactories_ should equal(taskPost.aggregatorFactories_)
    taskPre.indexSpec_ should equal(taskPost.indexSpec_)
    taskPre.master_ should equal(taskPost.master_)
    taskPre.properties_ should equal(taskPost.properties_)
    taskPre.targetPartitionSize_ should equal(taskPost.targetPartitionSize_)
    taskPre.getHadoopDependencyCoordinates should equal(taskPost.getHadoopDependencyCoordinates)
    taskPre.getHadoopDependencyCoordinates should equal(hadoopDependencyCoordinates)
    taskPre.getBuildV9Directly should equal(taskPost.getBuildV9Directly)
  }

  it should "properly deserialize" in {
    val taskPre: SparkBatchIndexTask = buildSparkBatchIndexTask()
    val task: Task = objectMapper.readValue(TaskConfProvider.taskConfURL, classOf[Task])
    task.getContext shouldBe 'Empty
    assertResult(SparkBatchIndexTask.TASK_TYPE_BASE)(task.getType)

    /** https://github.com/druid-io/druid/issues/2914
      * taskPre should ===(task)
      */
    task.asInstanceOf[SparkBatchIndexTask].getDataSchema.getParser.getParseSpec should
      ===(taskPre.getDataSchema.getParser.getParseSpec)
    task.asInstanceOf[SparkBatchIndexTask].getHadoopDependencyCoordinates.asScala should
      ===(Seq("org.apache.spark:spark-core_2.10:1.6.1-mmx0"))
    task.asInstanceOf[SparkBatchIndexTask].getBuildV9Directly should equal(false)
  }

  it should "be equal for equal tasks" in {
    val task1 = buildSparkBatchIndexTask()
    val task2 = buildSparkBatchIndexTask()
    task1 should equal(task2)
    task2 should equal(task1)

    task1 should equal(
      buildSparkBatchIndexTask(context = Map())
    )

    task1 should equal(
      buildSparkBatchIndexTask(indexSpec = new IndexSpec())
    )
  }

  it should "not be equal for dissimilar tasks" in {
    val task1 = buildSparkBatchIndexTask()
    task1 should not equal buildSparkBatchIndexTask(id = taskId + "something else")

    /** DataSchema compare is busted
      * task1 should not equal
      * buildSparkBatchIndexTask(dataSchema = buildDataSchema(dataSource = dataSource + "something else"))
      */

    task1 should not equal buildSparkBatchIndexTask(dataFiles = dataFiles ++ List("something else"))

    task1 should not equal buildSparkBatchIndexTask(rowsPerPartition = rowsPerPartition + 1)


    task1 should not equal buildSparkBatchIndexTask(rowsPerPersist = rowsPerFlush + 1)

    task1 should not equal buildSparkBatchIndexTask(master = master + "something else")

    val notSameProperties = new Properties(properties)
    notSameProperties.setProperty("Something not present", "some value")
    task1 should not equal buildSparkBatchIndexTask(properties = notSameProperties)

    task1 should
      not equal
      buildSparkBatchIndexTask(
        indexSpec = new IndexSpec(
          new RoaringBitmapSerdeFactory(false),
          CompressionStrategy.LZ4,
          CompressionStrategy.LZ4,
          null
        )
      )

    task1 should not equal buildSparkBatchIndexTask(context = Map[String, Object]("test" -> "oops"))

    task1 should not equal buildSparkBatchIndexTask(classpathPrefix = "someOther.jar")

    task1 should not equal buildSparkBatchIndexTask(buildV9Directly = false)
  }

  it should "default buildV9Directly to false if not specified" in {

    val taskWithoutV9 = buildSparkBatchIndexTaskWithoutV9()
    taskWithoutV9.getBuildV9Directly should equal(false)
  }
}
