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
import com.google.common.base.{Preconditions, Strings}
import com.google.common.collect.Iterables
import com.google.inject.Injector
import com.metamx.common.logger.Logger
import io.druid.common.utils.JodaUtils
import io.druid.data.input.impl.ParseSpec
import io.druid.granularity.QueryGranularity
import io.druid.guice.{ExtensionsConfig, GuiceInjectors}
import io.druid.indexing.common.actions.{LockTryAcquireAction, TaskActionClient}
import io.druid.indexing.common.task.{AbstractTask, HadoopTask}
import io.druid.indexing.common.{TaskStatus, TaskToolbox}
import io.druid.initialization.Initialization
import io.druid.query.aggregation.AggregatorFactory
import io.druid.segment.IndexSpec
import io.druid.segment.indexing.DataSchema
import io.druid.timeline.DataSegment
import io.tesla.aether.internal.DefaultTeslaAether
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.Interval

import scala.collection.JavaConversions._

@JsonCreator
class SparkBatchIndexTask(
  @JsonProperty("id")
  id: String,
  @JsonProperty("dataSchema")
  dataSchema: DataSchema,
  @JsonProperty("intervals")
  intervals: util.List[Interval],
  @JsonProperty("paths")
  dataFiles: util.List[String],
  @JsonProperty("targetPartitionSize")
  targetPartitionSize: Long = SparkBatchIndexTask.DEFAULT_TARGET_PARTITION_SIZE,
  @JsonProperty("maxRowsInMemory")
  rowFlushBoundary: Int = SparkBatchIndexTask.DEFAULT_ROW_FLUSH_BOUNDARY,
  @JsonProperty("properties")
  properties: Properties = new Properties(),
  @JsonProperty("master")
  master: String = "local[1]",
  @JsonProperty("context")
  context: util.Map[String, Object] = Map[String, Object](),
  @JsonProperty("indexSpec")
  indexSpec: IndexSpec = new IndexSpec(),
  @JsonProperty("classpathPrefix")
  classpathPrefix: String = null
) extends HadoopTask(
  if (id == null) {
    AbstractTask
      .makeId(
        null, SparkBatchIndexTask.TASK_TYPE, dataSchema.getDataSource,
        JodaUtils
          .umbrellaInterval(JodaUtils.condenseIntervals(Preconditions.checkNotNull(intervals, "%s", "intervals")))
      )
  }
  else {
    id
  },
  dataSchema.getDataSource,
  List[String](
    // TODO: better handle scala version here
    "%s:%s_2.10:%s" format
      (classOf[SparkBatchIndexTask].getPackage.getImplementationVendor, classOf[SparkBatchIndexTask].getPackage
        .getImplementationTitle, classOf[SparkBatchIndexTask].getPackage.getImplementationVersion)
  ),
  if (context == null) {
    Map[String, String]()
  }
  else {
    context
  }
)
{
  val log                  : Logger                            = new Logger(classOf[SparkBatchIndexTask])
  val properties_          : Properties                        =
    if (properties == null) {
      new Properties()
    } else {
      properties
    }
  val targetPartitionSize_ : Long                              =
    if (targetPartitionSize == 0) {
      SparkBatchIndexTask.DEFAULT_TARGET_PARTITION_SIZE
    } else {
      targetPartitionSize
    }
  val aggregatorFactories_ : java.util.List[AggregatorFactory] =
    if (dataSchema.getAggregators == null) {
      List()
    } else {
      dataSchema.getAggregators.toList
    }
  val rowFlushBoundary_    : Int                               =
    if (rowFlushBoundary == 0) {
      SparkBatchIndexTask.DEFAULT_ROW_FLUSH_BOUNDARY
    } else {
      rowFlushBoundary
    }
  val master_              : String                            =
    if (master == null) {
      "local[1]"
    } else {
      master
    }
  val indexSpec_           : IndexSpec                         =
    if (indexSpec == null) {
      new IndexSpec()
    } else {
      indexSpec
    }

  override def getType: String = SparkBatchIndexTask.TASK_TYPE

  override def run(toolbox: TaskToolbox): TaskStatus = {
    Preconditions.checkNotNull(dataFiles, "%s", "paths")
    Preconditions.checkArgument(!dataFiles.isEmpty, "%s", "empty dataFiles")
    Preconditions.checkNotNull(Strings.emptyToNull(dataSchema.getDataSource), "%s", "dataSource")
    Preconditions.checkNotNull(dataSchema.getParserMap, "%s", "parseSpec")
    Preconditions.checkNotNull(dataSchema.getGranularitySpec.getQueryGranularity, "%s", "queryGranularity")
    Preconditions.checkNotNull(dataSchema.getGranularitySpec.getSegmentGranularity, "%s", "segmentGranularity")

    var status: Option[TaskStatus] = Option.empty

    try {
      val outputPath = toolbox.getSegmentPusher.getPathForHadoop(getDataSource)
      val classLoader = buildClassLoader(toolbox)
      val task = SerializedJsonStatic.mapper.writeValueAsString(this)
      log.debug("Sending task `%s`", task)

      val result = HadoopTask.invokeForeignLoader[util.ArrayList[String], util.ArrayList[String]](
        "io.druid.indexer.spark.Runner",
        new util.ArrayList(List(task, Iterables.getOnlyElement(getTaskLocks(toolbox)).getVersion, outputPath)),
        classLoader
      )
      toolbox.pushSegments(result.map(SerializedJsonStatic.mapper.readValue(_, classOf[DataSegment])))
      status = Option.apply(TaskStatus.success(getId))
    }
    catch {
      case e: Throwable => log.error(e, "%s", "Error running task [%s]" format getId)
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
    Objects.equals(getId, other.getId) &&
      Objects.equals(getDataSchema, other.getDataSchema) &&
      Objects.equals(getIntervals, other.getIntervals) &&
      Objects.equals(getDataFiles, other.getDataFiles) &&
      Objects.equals(getTargetPartitionSize, other.getTargetPartitionSize) &&
      Objects.equals(getRowFlushBoundary, other.getRowFlushBoundary) &&
      Objects.equals(getProperties, other.getProperties) &&
      Objects.equals(getMaster, other.getMaster) &&
      Objects.equals(getContext, other.getContext) &&
      Objects.equals(getIndexSpec, other.getIndexSpec) &&
      Objects.equals(getClasspathPrefix, other.getClasspathPrefix)
  }

  @throws(classOf[Exception])
  override def isReady(taskActionClient: TaskActionClient): Boolean = taskActionClient
    .submit(new LockTryAcquireAction(totalInterval))
    .isPresent

  @JsonProperty("id")
  override def getId = super.getId

  @JsonProperty("dataSchema")
  def getDataSchema = dataSchema

  @JsonProperty("intervals")
  def getIntervals = intervals

  @JsonProperty("paths")
  def getDataFiles = dataFiles

  @JsonProperty("targetPartitionSize")
  def getTargetPartitionSize = targetPartitionSize_

  @JsonProperty("maxRowsInMemory")
  def getRowFlushBoundary = rowFlushBoundary_

  @JsonProperty("properties")
  def getProperties = properties_

  @JsonProperty("master")
  def getMaster = master_

  @JsonProperty("context")
  override def getContext = super.getContext

  @JsonProperty("indexSpec")
  def getIndexSpec = indexSpec_

  lazy val totalInterval: Interval = JodaUtils.umbrellaInterval(JodaUtils.condenseIntervals(intervals))

  @JsonProperty("classpathPrefix")
  override def getClasspathPrefix = classpathPrefix

  override def toString = s"SparkBatchIndexTask($getType, $getId, $getDataSchema, $getIntervals, $getDataFiles, $getTargetPartitionSize, $getRowFlushBoundary, $getProperties, $getMaster, $getContext, $getIndexSpec, $getClasspathPrefix)"
}

object SparkBatchIndexTask
{
  private val DEFAULT_ROW_FLUSH_BOUNDARY   : Int    = 80000
  private val DEFAULT_TARGET_PARTITION_SIZE: Long   = 5000000L
  private val CHILD_PROPERTY_PREFIX        : String = "druid.indexer.fork.property."
  val KRYO_CLASSES = Array(
    classOf[SerializedJson[DataSegment]],
    classOf[SerializedJson[QueryGranularity]],
    classOf[SerializedJson[QueryGranularity]],
    classOf[SerializedJson[AggregatorFactory]],
    classOf[SerializedJson[ParseSpec]],
    classOf[SerializedJson[IndexSpec]],
    classOf[SerializedJson[DataSchema]]
  ).asInstanceOf[Array[Class[_]]]
  val log          = new Logger(SparkBatchIndexTask.getClass)
  val TASK_TYPE    = "index_spark"

  def runTask(args: java.util.ArrayList[String]): java.util.ArrayList[String] = {
    val task = SerializedJsonStatic.mapper.readValue(args.get(0), classOf[SparkBatchIndexTask])
    val conf = new SparkConf()
      .setAppName(task.getId)
      .setMaster(task.getMaster)
      .set("spark.executor.memory", "7G")
      .set("spark.executor.cores", "1")
      .set("spark.kryo.referenceTracking", "false")
      .set("user.timezone", "UTC")
      .set("file.encoding", "UTF-8")
      .set("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager")
      .set("org.jboss.logging.provider", "slf4j")
      .set("druid.processing.columnCache.sizeBytes", "1000000000")
      .set("druid.extensions.searchCurrentClassloader", "true")
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
        task.getProperties.entrySet().map(x => Seq(x.getKey.toString, x.getValue.toString).mkString(":")).mkString(",")
      )
    val sc = new SparkContext(conf.setAll(task.getProperties))
    try {
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

      classpathProperty.split(File.pathSeparator).filter(_.endsWith(".jar")).foreach(
        x => {
          log.info("Adding path jar [%s]", x)
          sc.addJar(x)
        }
      )

      SparkContext.jarOfClass(classOf[SparkBatchIndexTask]).filter(_.endsWith(".jar")).foreach(
        x => {
          log.info("Adding class jar [%s]", x)
          sc.addJar(x)
        }
      )

      extensionJars.filter(_.endsWith(".jar")).foreach(
        x => {
          log.info("Adding extension jar [%s]", x)
          sc.addJar(x)
        }
      )

      val version = args.get(1)
      val outputPath = args.get(2)
      log.debug("Using version [%s]", version)

      val dataSegments = SparkDruidIndexer.loadData(
        task.getDataFiles,
        new SerializedJson[DataSchema](task.getDataSchema),
        task.totalInterval,
        task.getTargetPartitionSize,
        task.getRowFlushBoundary,
        outputPath,
        task.getIndexSpec,
        sc
      ).map(_.withVersion(version))
      log.info("Found segments `%s`", util.Arrays.deepToString(dataSegments.toArray))
      new util.ArrayList(dataSegments.map(SerializedJsonStatic.mapper.writeValueAsString))
    }
    finally {
      sc.stop()
    }
  }
}
