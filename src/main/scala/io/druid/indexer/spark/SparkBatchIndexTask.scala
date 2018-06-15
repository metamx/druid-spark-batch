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

import java.io.{Closeable, File, IOException, PrintWriter}
import java.nio.file.Files
import java.util
import java.util.{Objects, Properties}
import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.google.common.base.Joiner
import com.google.common.base.{Preconditions, Strings}
import com.google.common.collect.Iterables
import com.google.common.io.Closer
import io.druid.data.input.impl.ParseSpec
import io.druid.indexing.common.actions.LockListAction
import io.druid.indexing.common.{TaskStatus, TaskToolbox}
import io.druid.indexing.common.actions.{LockTryAcquireAction, TaskActionClient}
import io.druid.indexing.common.task.HadoopTask
import io.druid.java.util.common.DateTimes
import io.druid.java.util.common.JodaUtils
import io.druid.java.util.common.granularity._
import io.druid.java.util.common.logger.Logger
import io.druid.query.aggregation.AggregatorFactory
import io.druid.segment.IndexSpec
import io.druid.segment.indexing.DataSchema
import io.druid.timeline.DataSegment
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
  classpathPrefix: String = null,
  @JsonProperty("hadoopDependencyCoordinates")
  hadoopDependencyCoordinates: util.List[String] = null,
  @JsonProperty("buildV9Directly")
  buildV9Directly: Boolean = false
) extends HadoopTask(
  if (id == null) {
    SparkBatchIndexTask.getOrMakeId(
        null, SparkBatchIndexTask.TASK_TYPE_BASE, dataSchema.getDataSource,
        JodaUtils
          .umbrellaInterval(JodaUtils.condenseIntervals(Preconditions.checkNotNull(intervals, "%s", "intervals")))
      )
  }
  else {
    id
  },
  dataSchema.getDataSource,
  hadoopDependencyCoordinates,
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

  override def getType: String = SparkBatchIndexTask.TASK_TYPE_BASE

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
      val task = toolbox.getObjectMapper.writeValueAsString(this)
      log.debug("Sending task `%s`", task)

      val result = HadoopTask.invokeForeignLoader[util.ArrayList[String], util.ArrayList[String]](
        "io.druid.indexer.spark.Runner",
        new util.ArrayList(List(
          task,
          Iterables.getOnlyElement(toolbox.getTaskActionClient.submit(new LockListAction())).getVersion,
          outputPath
        )),
        classLoader
      )
      toolbox.publishSegments(result.map(toolbox.getObjectMapper.readValue(_, classOf[DataSegment])))
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
      Objects.equals(getClasspathPrefix, other.getClasspathPrefix) &&
      Objects.equals(getBuildV9Directly, other.getBuildV9Directly)
  }

  // This is kind of weird to get a lock that's not based on granularityIntervals but its how the hadoop indexer did it
  // So we maintain that behavior for now
  lazy val lockInterval: Interval = JodaUtils.umbrellaInterval(JodaUtils.condenseIntervals(intervals))

  @throws(classOf[Exception])
  override def isReady(taskActionClient: TaskActionClient): Boolean = taskActionClient
    .submit(new LockTryAcquireAction(null, lockInterval)) != null

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

  @JsonProperty("classpathPrefix")
  override def getClasspathPrefix = classpathPrefix

  @JsonProperty("hadoopDependencyCoordinates")
  def getHadoopCoordinates = hadoopDependencyCoordinates

  @JsonProperty("buildV9Directly")
  def getBuildV9Directly = buildV9Directly

  override def toString = s"SparkBatchIndexTask($getType, $getId, $getDataSchema, $getIntervals, $getDataFiles, " +
    s"$getTargetPartitionSize, $getRowFlushBoundary, $getProperties, $getMaster, $getContext, $getIndexSpec, " +
    s"$getClasspathPrefix, $getBuildV9Directly)"
}

object SparkBatchIndexTask
{
  private val DEFAULT_ROW_FLUSH_BOUNDARY   : Int    = 75000
  private val DEFAULT_TARGET_PARTITION_SIZE: Long   = 5000000L
  private val CHILD_PROPERTY_PREFIX        : String = "druid.indexer.fork.property."
  val log            = new Logger(SparkBatchIndexTask.getClass)
  val TASK_TYPE_BASE = "index_spark"
  private val ID_JOINER = Joiner.on("_")

  def mapToSegmentIntervals(originalIntervals: Iterable[Interval], granularity: Granularity): Iterable[Interval] = {
    originalIntervals.map(x => iterableAsScalaIterable(granularity.getIterable(x))).reduce(_ ++ _)
  }

  // Properties which do not carry over to executors
  private[SparkBatchIndexTask] val forbiddenProperties = Seq(
    "druid.extensions.coordinates",
    "druid.extensions.loadList"
  )

  // See io.druid.indexing.overlord.config.ForkingTaskRunnerConfig.allowedPrefixes
  // We don't include tmp.dir
  // user.timezone and file.encoding are set above
  private[SparkBatchIndexTask] val allowedPrefixes = Seq(
    "com.metamx",
    "druid",
    "io.druid",
    "hadoop"
  )

  def getKryoClasses() = Array(
    classOf[SerializedHadoopConfig],
    classOf[SerializedJson[DataSegment]],
    classOf[SerializedJson[Granularity]],
    classOf[SerializedJson[AggregatorFactory]],
    classOf[SerializedJson[ParseSpec]],
    classOf[SerializedJson[IndexSpec]],
    classOf[SerializedJson[DataSchema]]
  ).asInstanceOf[Array[Class[_]]]

  def runTask(args: java.util.ArrayList[String]): java.util.ArrayList[String] = {
    val task = SerializedJsonStatic.mapper.readValue(args.get(0), classOf[SparkBatchIndexTask])
    val conf = new SparkConf()
      .setAppName(task.getId)
      .setMaster(task.getMaster)
      // These can screw with resource scheduling and may be worth removing
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
      .registerKryoClasses(SparkBatchIndexTask.getKryoClasses())

    val propertiesToSet = new Properties()
    propertiesToSet.setProperty("druid.extensions.searchCurrentClassloader", "true")

    val propertiesTempDir = Files.createTempDirectory("druid_spark_properties_").toFile
    log.debug("Using properties path [%s]", propertiesTempDir)
    val propertiesFile = new File(propertiesTempDir, "runtime.properties")

    val closer: Closer = Closer.create()

    closer.register(
      new Closeable
      {
        @throws[IOException] override def close(): Unit = {
          if (propertiesTempDir.exists() && !propertiesTempDir.delete()) {
            log.info(
              new IOException("Unable to delete %s" format propertiesTempDir),
              "Could note cleanup temporary directory"
            )
          }
        }
      }
    )

    closer.register(
      new Closeable
      {
        @throws[IOException] override def close(): Unit = {
          if (propertiesFile.exists() && !propertiesFile.delete()) {
            log.info(
              new IOException("Unable to delete %s" format propertiesFile),
              "Could not clean up temporary properties file"
            )
          }
        }
      }
    )

    System.getProperties.stringPropertyNames().filter(
      x => {
        allowedPrefixes.exists(x.startsWith)
      }
    ).foreach(
      x => {
        log.info("Setting io.druid property [%s]", x)
        propertiesToSet.setProperty(x, System.getProperty(x))
      }
    )
    System.getProperties.stringPropertyNames().filter(_.startsWith(CHILD_PROPERTY_PREFIX)).foreach(
      x => {
        val y = x.substring(CHILD_PROPERTY_PREFIX.length)
        log.info("Setting child property [%s]", y)
        propertiesToSet.setProperty(y, System.getProperty(x))
      }
    )

    log.info(
      "Adding task properties: [%s]",
      task.getProperties.entrySet().map(x => Seq(x.getKey.toString, x.getValue.toString).mkString(":")).mkString(",")
    )
    task.getProperties.foreach(x => propertiesToSet.setProperty(x._1, x._2))

    forbiddenProperties.foreach(propertiesToSet.remove)

    val sc = new SparkContext(conf.setAll(propertiesToSet))
    closer.register(
      new Closeable
      {
        override def close(): Unit = sc.stop()
      }
    )

    try {

      val propertyCloser: Closer = Closer.create()
      val printWriter = propertyCloser.register(new PrintWriter(propertiesFile))
      // Make a file to propagate to all nodes which contains the properties
      try {
        propertiesToSet.toSeq.foreach(x => printWriter.println(Seq(x._1, x._2).mkString("=")))
      }
      catch {
        case t: Throwable => throw propertyCloser.rethrow(t)
      }
      finally {
        propertyCloser.close()
      }

      sc.addJar(propertiesFile.toURI.toString)

      System.getProperties.stringPropertyNames().filter(_.startsWith(CHILD_PROPERTY_PREFIX)).foreach(
        x => {
          val y = x.substring(CHILD_PROPERTY_PREFIX.length)
          log.debug("Setting child hadoop property [%s]", y)
          sc.hadoopConfiguration.set(y, System.getProperty(x), "Druid Forking Property")
        }
      )

      // Should be set by HadoopTask for job jars
      // Hadoop tasks use io.druid.indexer.JobHelper::setupClasspath to replicate their jars.
      // The jars are put in some universally accessable location and each job addresses them.
      // Spark has internal mechanisms for shipping its jars around, making an external blob storage (like HDFS or S3)
      // not required for jar distribution.
      // In general they both "make list of jars... distribute via XXX... make that list available on all the workers"
      // but how they accomplish the XXX part is different.
      // So this **can't** use io.druid.indexer.JobHelper::setupClasspath, and has to have its own way of parsing here.
      val hadoopTaskJars = System.getProperties.toMap
        .getOrElse(
          "druid.hadoop.internal.classpath",
          throw new IllegalStateException("missing druid.hadoop.internal.classpath from HadoopTask")
        )
      hadoopTaskJars.split(File.pathSeparator).filter(_.endsWith(".jar")).foreach(
        x => {
          log.info(s"Adding jar [$x]")
          sc.addJar(x)
        }
      )

      val version = args.get(1)
      val outputPath = args.get(2)
      log.debug("Using version [%s]", version)

      val dataSegments = SparkDruidIndexer.loadData(
        task.getDataFiles,
        new SerializedJson[DataSchema](task.getDataSchema),
        SparkBatchIndexTask
          .mapToSegmentIntervals(task.getIntervals, task.getDataSchema.getGranularitySpec.getSegmentGranularity),
        task.getTargetPartitionSize,
        task.getRowFlushBoundary,
        outputPath,
        task.getIndexSpec,
        task.getBuildV9Directly,
        sc
      ).map(_.withVersion(version))
      log.info("Found segments `%s`", util.Arrays.deepToString(dataSegments.toArray))
      new util.ArrayList(dataSegments.map(SerializedJsonStatic.mapper.writeValueAsString))
    }
    catch {
      case t: Throwable => throw closer.rethrow(t)
    }
    finally {
      closer.close()
    }
  }

  private[SparkBatchIndexTask] def getOrMakeId(
    id: String,
    typeName: String,
    dataSource: String,
    interval: Interval
  ): String = {
    if (id != null) return id
    val objects = new util.ArrayList[AnyRef]
    objects.add(typeName)
    objects.add(dataSource)
    if (interval != null) {
      objects.add(interval.getStart)
      objects.add(interval.getEnd)
    }
    objects.add(DateTimes.nowUtc.toString)
    ID_JOINER.join(objects)
  }
}
