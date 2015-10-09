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

import java.io.File
import java.net.{URL, URLClassLoader}
import java.util
import java.util.{Objects, Properties}

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.google.common.base.{Optional, Preconditions, Strings}
import com.google.common.collect.{ImmutableMap, Iterables}
import com.google.inject.Injector
import com.metamx.common.logger.Logger
import io.druid.data.input.impl.ParseSpec
import io.druid.granularity.QueryGranularity
import io.druid.guice.{ExtensionsConfig, GuiceInjectors}
import io.druid.indexing.common.actions.{LockTryAcquireAction, TaskActionClient}
import io.druid.indexing.common.task.AbstractTask
import io.druid.indexing.common.{TaskLock, TaskStatus, TaskToolbox}
import io.druid.initialization.Initialization
import io.druid.query.aggregation.AggregatorFactory
import io.druid.timeline.DataSegment
import io.tesla.aether.internal.DefaultTeslaAether
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.Interval

import scala.collection.JavaConversions._

@JsonCreator
class SparkBatchIndexTask(
  @JsonProperty("id")
  id: String,
  @JsonProperty("dataSource")
  dataSource: String,
  @JsonProperty("interval")
  interval: Interval,
  @JsonProperty("dataFiles")
  dataFiles: util.List[String],
  @JsonProperty("parseSpec")
  parseSpec: ParseSpec,
  @JsonProperty("outputPath")
  outPathString: String,
  @JsonProperty("metrics")
  aggregatorFactories: java.util.List[AggregatorFactory] = List(),
  @JsonProperty("rowsPerPartition")
  rowsPerPartition: Long = 5000000,
  @JsonProperty("rowsFlushBoundary")
  rowsPerPersist: Int = 80000,
  @JsonProperty("properties")
  properties: Properties =
  Seq(
    ("user.timezone", "UTC"),
    ("file.encoding", "UTF-8"),
    ("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager"),
    ("org.jboss.logging.provider", "slf4j"),
    ("druid.processing.columnCache.sizeBytes", "1000000000"),
    ("druid.extensions.searchCurrentClassloader", "true")
  ).foldLeft(new Properties())(
    (p, e) => {
      p.setProperty(e._1, e._2)
      p
    }
  ),
  @JsonProperty("master")
  master: String = "local[1]",
  @JsonProperty("queryGranularity")
  queryGranularity: QueryGranularity,
  @JsonProperty("context")
  context: util.Map[String, Object] = Map[String, Object]()
  ) extends AbstractTask(
  if (id == null) {
    AbstractTask
      .makeId(null, SparkBatchIndexTask.TASK_TYPE, dataSource, interval)
  }
  else {
    id
  }, dataSource,
  if (context == null) {
    ImmutableMap.of()
  }
  else {
    context
  }
)
{
  private val CHILD_PROPERTY_PREFIX: String = "druid.indexer.fork.property."
  val log                  : Logger                            = new Logger(classOf[SparkBatchIndexTask])
  val properties_          : Properties                        =
    if (properties == null) {
      Seq(
        ("user.timezone", "UTC"),
        ("file.encoding", "UTF-8"),
        ("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager"),
        ("org.jboss.logging.provider", "slf4j"),
        ("druid.processing.columnCache.sizeBytes", "1000000000"),
        ("druid.extensions.searchCurrentClassloader", "true")
      ).foldLeft(new Properties())(
        (p, e) => {
          p.setProperty(e._1, e._2)
          p
        }
      )
    } else {
      properties
    }
  val rowsPerPartition_    : Long                              =
    if (rowsPerPartition == 0) {
      5000000L
    } else {
      rowsPerPartition
    }
  val aggregatorFactories_ : java.util.List[AggregatorFactory] =
    if (aggregatorFactories == null) {
      List()
    } else {
      aggregatorFactories
    }
  val rowsPerPersist_      : Int                               =
    if (rowsPerPersist == 0) {
      80000
    } else {
      rowsPerPersist
    }
  val master_              : String                            =
    if (master == null) {
      "local[1]"
    } else {
      master
    }

  override def getType: String = SparkBatchIndexTask.TASK_TYPE

  override def run(toolbox: TaskToolbox): TaskStatus = {

    var optionalSC : Option[SparkContext] = Option.empty
    var status : Option[TaskStatus] = Option.empty
    val priorLoader = Thread.currentThread.getContextClassLoader
    try {
      Thread.currentThread.setContextClassLoader(classOf[SerializedJson[ParseSpec]].getClassLoader)

      Preconditions.checkNotNull(dataFiles, "%s", "dataFiles")
      Preconditions.checkArgument(!dataFiles.isEmpty, "%s", "empty dataFiles")
      Preconditions.checkNotNull(Strings.emptyToNull(dataSource), "%s", "dataSource")
      Preconditions.checkNotNull(parseSpec, "%s", "parseSpec")
      Preconditions.checkNotNull(interval, "%s", "interval")
      Preconditions.checkNotNull(Strings.emptyToNull(outPathString), "%s", "outputPath")
      Preconditions.checkNotNull(queryGranularity, "%s", "queryGranularity")
      log.debug("Sending task `%s`", SerializedJsonStatic.mapper.writeValueAsString(this))


      val conf = new SparkConf()
        .setAppName(getId)
        .setMaster(master_)
        // TODO: better config here
        .set("spark.executor.memory", "6G")
        .set("spark.executor.cores", "2")
        .set("spark.kryo.referenceTracking", "false")
        // registerKryoClasses already does the below two lines
        //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        //.set("spark.kryo.classesToRegister", SparkBatchIndexTask.KRYO_CLASSES.map(_.getCanonicalName).mkString(","))
        .registerKryoClasses(SparkBatchIndexTask.KRYO_CLASSES)

      System.getProperties.stringPropertyNames().filter(_.startsWith("io.druid")).foreach(
        x => {
          log.debug("Setting io.druid property [%s]", x)
          conf.set(x, System.getProperty(x))
        }
      )
      System.getProperties.stringPropertyNames().filter(_.startsWith(CHILD_PROPERTY_PREFIX)).foreach(
        x => {
          val y = x.substring(CHILD_PROPERTY_PREFIX.length)
          log.debug("Setting child property [%s]", y)
          conf.set(y, System.getProperty(x))
        }
      )

      log
        .debug(
          "Adding properties: [%s]",
          properties_.entrySet().map(x => Seq(x.getKey.toString, x.getValue.toString).mkString(":")).mkString(",")
        )
      val sc = new SparkContext(conf.setAll(properties_))
      optionalSC = Option.apply(sc)

      System.getProperties.stringPropertyNames().filter(_.startsWith(CHILD_PROPERTY_PREFIX)).foreach(
        x => {
          val y = x.substring(CHILD_PROPERTY_PREFIX.length)
          log.debug("Setting child hadoop property [%s]", y)
          sc.hadoopConfiguration.set(y, System.getProperty(x), "Druid Forking Property")
        }
      )

      val injector: Injector = GuiceInjectors.makeStartupInjector
      val extensionsConfig: ExtensionsConfig = injector.getInstance(classOf[ExtensionsConfig])
      val aetherClient: DefaultTeslaAether = Initialization.getAetherClient(extensionsConfig)

      val extensionJars = extensionsConfig.getCoordinates.flatMap(
        x => {
          val coordinateLoader: ClassLoader = Initialization
            .getClassLoaderForCoordinates(aetherClient, x, extensionsConfig.getDefaultVersion)
          coordinateLoader.asInstanceOf[URLClassLoader].getURLs
        }
      ).foldLeft(List[URL]())(_ ++ List(_)).map(_.toString)

      var classpathProperty: String = System.getProperty("druid.hadoop.internal.classpath")
      if (classpathProperty == null) {
        classpathProperty = System.getProperty("java.class.path")
      }

      classpathProperty.split(File.pathSeparator).foreach(
        x => {
          log.info("Adding path jar [%s]", x)
          sc.addJar(x)
        }
      )

      SparkContext.jarOfClass(classOf[SparkBatchIndexTask]).foreach(
        x => {
          log.info("Adding class jar [%s]", x)
          sc.addJar(x)
        }
      )

      extensionJars.foreach(
        x => {
          log.info("Adding extension jar [%s]", x)
          sc.addJar(x)
        }
      )

      val myLock: TaskLock = Iterables.getOnlyElement(getTaskLocks(toolbox))
      val version = myLock.getVersion
      log.debug("Using version [%s]", version)

      val dataSegments = SparkDruidIndexer.loadData(
        dataFiles,
        dataSource,
        new SerializedJson[ParseSpec](parseSpec),
        interval,
        aggregatorFactories_,
        rowsPerPartition_,
        rowsPerPersist_,
        outPathString,
        getQueryGranularity,
        sc
      ).map(_.withVersion(version))
      log.info("Found segments `%s`", util.Arrays.deepToString(dataSegments.toArray))
      toolbox.pushSegments(dataSegments)
      status = Option.apply(TaskStatus.success(getId))
    }
    catch {
      case t: Throwable => SparkBatchIndexTask.log.error(t, "Error in task [%s]", getId)
    }
    finally {
      Thread.currentThread.setContextClassLoader(priorLoader)
      optionalSC match {
        case Some(x) => x.stop()
        case None => log.info("No spark context.")
      }
    }
    status match {
      case Some(x) => x
      case None => TaskStatus.failure(getId)
    }
  }

  override def equals(o: Any): Boolean = {
    if (!super.equals(o)) {
      return false
    }
    if (o == null || this == null) {
      return false
    }
    if (!o.isInstanceOf[SparkBatchIndexTask]) {
      return false
    }
    val other = o.asInstanceOf[SparkBatchIndexTask]
    this.getAggregatorFactories.equals(other.getAggregatorFactories) &&
      Objects.equals(getDataFile, other.getDataFile) &&
      Objects.equals(getFormattedDataSource, other.getFormattedDataSource) &&
      Objects.equals(getFormattedId, other.getFormattedId) &&
      Objects.equals(getMaster, other.getMaster) &&
      Objects.equals(getOutputPath, other.getOutputPath) &&
      Objects.equals(getProperties, other.getProperties) &&
      Objects.equals(getRowsPerPartition, other.getRowsPerPartition) &&
      Objects.equals(getRowsPerPersist, other.getRowsPerPersist) &&
      Objects.equals(getTotalInterval, other.getTotalInterval) &&
      Objects.equals(
        getParseSpec
          .getDimensionsSpec
          .getDimensionExclusions,
        other.getParseSpec.getDimensionsSpec.getDimensionExclusions
      ) &&
      Objects.equals(
        getParseSpec.getDimensionsSpec.getDimensions,
        other.getParseSpec.getDimensionsSpec.getDimensions
      ) &&
      Objects.equals(
        getParseSpec
          .getDimensionsSpec
          .getSpatialDimensions,
        other.getParseSpec.getDimensionsSpec.getSpatialDimensions
      ) &&
      Objects.equals(
        parseSpec.getTimestampSpec.getMissingValue,
        other.getParseSpec.getTimestampSpec.getMissingValue
      ) &&
      Objects.equals(
        parseSpec
          .getTimestampSpec
          .getTimestampColumn,
        other.getParseSpec.getTimestampSpec.getTimestampColumn
      ) &&
      Objects.equals(
        getParseSpec
          .getTimestampSpec
          .getTimestampFormat,
        other.getParseSpec.getTimestampSpec.getTimestampFormat
      ) &&
      Objects.equals(getQueryGranularity, other.getQueryGranularity)
  }

  @throws(classOf[Exception])
  override def isReady(taskActionClient: TaskActionClient): Boolean = taskActionClient
    .submit(new LockTryAcquireAction(interval))
    .isPresent

  @JsonProperty("id")
  def getFormattedId = getId

  @JsonProperty("dataSource")
  def getFormattedDataSource = getDataSource

  @JsonProperty("interval")
  def getTotalInterval = interval

  @JsonProperty("dataFiles")
  def getDataFile = dataFiles

  @JsonProperty("parseSpec")
  def getParseSpec = parseSpec

  @JsonProperty("metrics")
  def getAggregatorFactories = aggregatorFactories_

  @JsonProperty("outputPath")
  def getOutputPath = outPathString

  @JsonProperty("rowsPerPartition")
  def getRowsPerPartition = rowsPerPartition_

  @JsonProperty("rowsFlushBoundary")
  def getRowsPerPersist = rowsPerPersist_

  @JsonProperty("properties")
  def getProperties = properties_

  @JsonProperty("master")
  def getMaster = master_

  @JsonProperty("queryGranularity")
  def getQueryGranularity = queryGranularity
}

object SparkBatchIndexTask
{
  val KRYO_CLASSES = Array(
    classOf[SerializedHadoopConfig],
    classOf[SerializedJson[DataSegment]],
    classOf[SerializedJson[QueryGranularity]],
    classOf[SerializedJson[QueryGranularity]],
    classOf[SerializedJson[AggregatorFactory]],
    classOf[SerializedJson[ParseSpec]]
  ).asInstanceOf[Array[Class[_]]]
  val log          = new Logger(SparkBatchIndexTask.getClass)
  val TASK_TYPE    = "index_spark"
}
