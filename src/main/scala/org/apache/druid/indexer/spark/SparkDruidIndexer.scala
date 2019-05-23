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

import java.io._
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.io.Closer
import com.google.inject.name.Names
import com.google.inject.{Binder, Injector, Key, Module}
import org.apache.avro.generic.GenericRecord
import org.apache.druid.data.input.{InputRow, MapBasedInputRow, MapBasedRow}
import org.apache.druid.data.input.impl._
import org.apache.druid.guice.annotations.{Json, Self}
import org.apache.druid.guice.{GuiceInjectors, JsonConfigProvider}
import org.apache.druid.indexer.HadoopyStringInputRowParser
import org.apache.druid.initialization.Initialization
import org.apache.druid.java.util.common.{IAE, ISE}
import org.apache.druid.java.util.common.granularity.{Granularities, Granularity, GranularityType, PeriodGranularity}
import org.apache.druid.java.util.common.lifecycle.Lifecycle
import org.apache.druid.java.util.common.logger.Logger
import org.apache.druid.java.util.emitter.service.ServiceEmitter
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent
import org.apache.druid.query.aggregation.AggregatorFactory
import org.apache.druid.segment._
import org.apache.druid.segment.column.ColumnConfig
import org.apache.druid.segment.incremental.{IncrementalIndex, IncrementalIndexSchema}
import org.apache.druid.segment.indexing.DataSchema
import org.apache.druid.segment.loading.DataSegmentPusher
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory
import org.apache.druid.server.DruidNode
import org.apache.druid.timeline.DataSegment
import org.apache.druid.timeline.partition.{HashBasedNumberedShardSpec, NoneShardSpec, ShardSpec}
import org.apache.commons.io.FileUtils
import org.apache.druid.data.input.parquet.avro.ParquetAvroHadoopInputRowParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.parquet.avro.DruidParquetAvroReadSupport
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{AccumulableInfo, SparkListener, SparkListenerApplicationEnd, SparkListenerStageCompleted}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, SparkContext}
import org.joda.time.{DateTime, Interval, Period}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object SparkDruidIndexer {
  private val log = new Logger(getClass)

  def rewriteMapBasedInputRow(ir: InputRow) =
    ir match {
      case x: MapBasedInputRow => {
        val map = new util.HashMap[String, Any]()
        map.putAll(x.getEvent)
        // Rewrite rows because ObjectFlatteners can't be deserialized
        new MapBasedInputRow(x.getTimestamp, x.getDimensions, map.asInstanceOf[util.Map[String, Object]])
      }
      case x => x
    }

  def loadData(
                dataFiles: Seq[String],
                dataSchema: SerializedJson[DataSchema],
                ingestIntervals: Iterable[Interval],
                rowsPerPartition: Long,
                rowsPerPersist: Int,
                outPathString: String,
                indexSpec: IndexSpec,
                buildV9Directly: Boolean,
                sc: SparkContext
              ): Seq[DataSegment] = {
    val dataSource = dataSchema.getDelegate.getDataSource
    val lifecycle = SerializedJsonStatic.lifecycle
    val emitter = SerializedJsonStatic.emitter

    logInfo("Initializing emitter for metrics")
    sc.addSparkListener(new SparkListener() {
      // Emit metrics at the end of each stage
      override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
        val dimensions = Map(
          "appId" -> sc.applicationId,
          "dataSource" -> dataSource,
          "stageId" -> stageCompleted.stageInfo.stageId.toString,
          "intervals" -> ingestIntervals.mkString("[", ",", "]")
        )
        val accumulatedInfo = stageCompleted.stageInfo.accumulables.toMap.flatMap {
          case (_, AccumulableInfo(_, Some(name), _, Some(value: Long), _, _, _)) =>
            Some(name -> value)
          case _ =>
            None
        }
        accumulatedInfo foreach { case (aggName, value) =>
          logDebug("emitting metric: %s".format(aggName))
          val eventBuilder = ServiceMetricEvent.builder()
          dimensions foreach { case (n, v) => eventBuilder.setDimension(n, v) }
          emitter.emit(
            eventBuilder.build(aggName, value)
          )
        }
      }

      // Closes lifecycle when sc.stop() is called
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        lifecycle.stop()
      }
    })

    logInfo(s"Launching Spark task with jar version [${getClass.getPackage.getImplementationVersion}]")
    val dataSegmentVersion = DateTime.now().toString
    val hadoopConfig = new SerializedHadoopConfig(sc.hadoopConfiguration)
    val aggs: Array[SerializedJson[AggregatorFactory]] = dataSchema.getDelegate.getAggregators
      .map(x => new SerializedJson[AggregatorFactory](x))
    logInfo(s"Starting caching of raw data for [$dataSource] over intervals [$ingestIntervals]")
    val passableIntervals = ingestIntervals.foldLeft(Seq[Interval]())((a, b) => a ++ Seq(b)) // Materialize for passing

    val totalGZSize = dataFiles.map(
      s => {
        val p = new Path(s)
        val fs = p.getFileSystem(sc.hadoopConfiguration)
        fs.getFileStatus(p).getLen
      }
    ).sum
    val startingPartitions = (totalGZSize / (100L << 20)).toInt + 1

    logInfo(s"Splitting [$totalGZSize] gz bytes into [$startingPartitions] partitions")

    // Hadoopify the data so it doesn't look so silly in Spark's DAG
    // Input data is probably in gigantic files, so redistribute
    val baseData = dataSchema.getDelegate.getParser match {
      case x : StringInputRowParser =>
        getP[String, StringInputRowParser](sc.textFile(dataFiles mkString ","),
          (parser) => str => rewriteMapBasedInputRow(parser.parse(str)),
          (parser) => str => {
            parser.parseBatch(ByteBuffer.wrap(str.getBytes)).map(rewriteMapBasedInputRow)
          },
          dataSchema, startingPartitions, passableIntervals)
      case x: ParquetAvroHadoopInputRowParser => {
        val jobConf = new JobConf()
        ParquetInputFormat.setReadSupportClass(jobConf, classOf[DruidParquetAvroReadSupport])
        val rdd = sc.newAPIHadoopFile(dataFiles.mkString(","), classOf[ParquetInputFormat[GenericRecord]],
          classOf[Void], classOf[GenericRecord], jobConf).values

        getP[GenericRecord, ParquetAvroHadoopInputRowParser](rdd,
          (parser) => parser.parse _,
          (parser) => str => parser.parseBatch(str),
          dataSchema, startingPartitions, passableIntervals)
      }
      case x => {
        logTrace(
          "Could not figure out how to handle class " +
            s"[${x.getClass.getCanonicalName}]. " +
            "Hoping it can handle string input"
        )
        getP[String, InputRowParser[Any]](sc.textFile(dataFiles mkString ","),
          (parser) => parser.parse _,
          (parser) => str => parser.parseBatch(str),
          dataSchema, startingPartitions, passableIntervals)
      }
    }

    logInfo("Starting uniqes")
    val optionalDims: Option[Set[String]] = if (dataSchema.getDelegate.getParser.getParseSpec.getDimensionsSpec.hasCustomDimensions) {
      val parseSpec = dataSchema.getDelegate.getParser.getParseSpec
      Some(parseSpec.getDimensionsSpec.getDimensionNames.asScala.toSet)
    } else {
      None
    }

    val uniquesEvents: RDD[(Long, util.Map[String, AnyRef])] = optionalDims match {
      case Some(dims) => baseData.mapValues(_.filterKeys(dims.contains))
      case None => baseData.asInstanceOf[RDD[(Long, util.Map[String, AnyRef])]]
    }

    val partitionMap: Map[Long, Long] = uniquesEvents.countApproxDistinctByKey(
      0.05,
      new DateBucketPartitioner(
        dataSchema.getDelegate.getGranularitySpec.getSegmentGranularity,
        passableIntervals
      )
    ).map(
      x => dataSchema.getDelegate.getGranularitySpec
        .getSegmentGranularity
        .bucketStart(new DateTime(x._1))
        .getMillis -> x._2
    ).reduceByKey(_ + _).collect().toMap

    // Map key tuple is DateBucket, PartitionInBucket with map value of Partition #
    val hashToPartitionMap: Map[(Long, Long), Int] = getSizedPartitionMap(
      partitionMap,
      rowsPerPartition
    )

    // Get dimension values and dims per timestamp
    hashToPartitionMap.foreach {
      (a: ((Long, Long), Int)) => {
        logInfo(
          s"Date Bucket [${a._1._1}] with partition number [${a._1._2}]" +
            s" has total partition number [${a._2}]"
        )
      }
    }

    val indexSpec_passable = new SerializedJson[IndexSpec](indexSpec)

    val partitioned_data = baseData
      .map(_ -> 0)
      .partitionBy(
        new DateBucketAndHashPartitioner(
          dataSchema.getDelegate.getGranularitySpec.getSegmentGranularity,
          hashToPartitionMap,
          optionalDims,
          Some(dataSchema.getDelegate.getParser.getParseSpec.getDimensionsSpec.getDimensionExclusions.asScala.toSet)
        )
      )
      .mapPartitionsWithIndex(
        (index, it) => {

          val partitionNums = hashToPartitionMap.filter(_._2 == index).map(_._1._2.toInt).toSeq
          if (partitionNums.isEmpty) {
            throw new ISE(
              s"Partition [$index] not found in partition map. " +
                s"Valid parts: ${hashToPartitionMap.values}"
            )
          }
          if (partitionNums.length != 1) {
            throw new
                ISE(s"Too many partitions found for index [$index] Found $partitionNums")
          }
          val partitionNum = partitionNums.head
          val timeBucket = hashToPartitionMap.filter(_._2 == index).map(_._1._1).head
          val timeInterval = passableIntervals.filter(_.getStart.getMillis == timeBucket).head
          logInfo(s"Creating index [$partitionNum] for date range [$timeInterval]")
          val partitionCount = hashToPartitionMap.count(_._1._1 == timeBucket)

          val parser: InputRowParser[_] = SerializedJsonStatic.mapper
            .convertValue(dataSchema.getDelegate.getParserMap, classOf[InputRowParser[_]])

          val rows = it

          val pusher: DataSegmentPusher = SerializedJsonStatic.injector
            .getInstance(classOf[DataSegmentPusher])
          val tmpPersistDir = Files.createTempDirectory("persist").toFile
          val tmpMergeDir = Files.createTempDirectory("merge").toFile
          val closer = Closer.create()
          closer.register(
            new Closeable {
              override def close(): Unit = {
                FileUtils.deleteDirectory(tmpPersistDir)
              }
            }
          )
          closer.register(
            new Closeable {
              override def close(): Unit = {
                FileUtils.deleteDirectory(tmpMergeDir)
              }
            }
          )
          try {
            // Fail early if hadoop config is screwed up
            val hadoopConf = hadoopConfig.getDelegate
            val outPath = new Path(outPathString)
            val dimSpec = dataSchema.getDelegate.getParser.getParseSpec.getDimensionsSpec
            val excludedDims = dimSpec.getDimensionExclusions
            val finalDims: Option[util.List[String]] = if (dimSpec.hasCustomDimensions) Some(dimSpec.getDimensionNames.asScala) else None
            val finalStaticIndexer = StaticIndex.INDEX_MERGER_V9


            val groupedRows = rows.grouped(rowsPerPersist)
            val indices = groupedRows.map(rs => {
              // TODO: rewrite this without using IncrementalIndex, because IncrementalIndex bears a lot of overhead
              // to support concurrent querying, that is not needed in Spark
              val incrementalIndex = new IncrementalIndex.Builder()
                .setIndexSchema(
                  new IncrementalIndexSchema.Builder()
                    .withDimensionsSpec(parser)
                    .withQueryGranularity(
                      dataSchema.getDelegate.getGranularitySpec.getQueryGranularity
                    )
                    .withMetrics(aggs.map(_.getDelegate): _*)
                    .withMinTimestamp(timeBucket)
                    .build()
                )
                .setReportParseExceptions(true)
                .setMaxRowCount(rowsPerPersist)
                .buildOnheap()
              rs.foreach(r => {
                incrementalIndex.add(
                  incrementalIndex.formatRow(
                    new MapBasedInputRow(
                      r._1._1,
                      finalDims.getOrElse((r._1._2.keySet() -- excludedDims).toList.asJava),
                      r._1._2
                    )
                  )
                )
              })
              incrementalIndex
            })
            val indicesList = indices.map(
              incIndex => {
                val adapter = new QueryableIndexIndexableAdapter(
                  closer.register(
                    StaticIndex.INDEX_IO.loadIndex(
                      finalStaticIndexer
                        .persist(
                          incIndex,
                          timeInterval,
                          tmpPersistDir,
                          indexSpec_passable.getDelegate,
                          null
                        )
                    )
                  )
                )
                incIndex.close()
                adapter
              }
            ).toList
            val file = finalStaticIndexer.merge(
              indicesList,
              true,
              aggs.map(_.getDelegate),
              tmpMergeDir,
              indexSpec_passable.getDelegate
            )
            val allDimensions: util.List[String] = indicesList
              .map(_.getDimensionNames)
              .foldLeft(Set[String]())(_ ++ _)
              .toList
            logInfo(s"Found dimensions [${util.Arrays.deepToString(allDimensions.toArray)}]")
            val dataSegmentTemplate = new DataSegment(
              dataSource,
              timeInterval,
              dataSegmentVersion,
              null,
              allDimensions,
              aggs.map(_.getDelegate.getName).toList,
              if (partitionCount == 1) {
                NoneShardSpec.instance()
              } else {
                new HashBasedNumberedShardSpec(partitionNum, partitionCount, null, SerializedJsonStatic.mapper)
              },
              -1,
              -1
            )
            // See https://github.com/druid-io/druid/pull/5187 and https://github.com/druid-io/druid/pull/5692
            // It's unclear whether this should use unique path, but making to use, just in case
            val finalDataSegment = pusher.push(file, dataSegmentTemplate, true)
            logInfo(s"Finished pushing $finalDataSegment")
            Seq(new SerializedJson[DataSegment](finalDataSegment)).iterator
          }
          catch {
            case t: Throwable =>
              logError(s"Error in partition [$index]", t)
              throw closer.rethrow(t)
          }
          finally {
            closer.close()
          }
        }
      )
    val results = partitioned_data.collect().map(_.getDelegate)
    logInfo(s"Finished with ${util.Arrays.deepToString(results.map(_.toString))}")
    results.toSeq
  }

  private def delegateParser[T](dataSchema: SerializedJson[DataSchema])() = {
    dataSchema.getDelegate.getParser.asInstanceOf[T]
  }

  def getP[T, R <: InputRowParser[_]](rdd: RDD[T],
                                      parseFn: R => T => InputRow,
                                      parseBatchFn: R => T => Seq[InputRow],
                                      dataSchema: SerializedJson[DataSchema],
                                      startingPartitions: Int,
                                      passableIntervals: Seq[Interval]) = {
    rdd.filter { f =>
      val parser = dataSchema.getDelegate.getParser.asInstanceOf[R]
      passableIntervals.exists { i =>
        val row = parseFn(parser)(f)
        i.contains(row.getTimestamp)
      }
    }.repartition(startingPartitions)
      // Persist the strings only rather than the event map
      // We have to do the parsing twice this way, but serde of the map is killer as well
      .persist(StorageLevel.DISK_ONLY)
      .mapPartitions { it =>
        val queryGran = dataSchema.getDelegate.getGranularitySpec.getQueryGranularity
        val segmentGran = dataSchema.getDelegate.getGranularitySpec.getSegmentGranularity
        val parser = dataSchema.getDelegate.getParser.asInstanceOf[R]
        it.flatMap(parseBatchFn(parser)).map(
          r => {
            var k: Long = queryGran.bucketStart(new DateTime(r.getTimestampFromEpoch)).getMillis
            if (k < 0) {
              // Example: AllGranularity
              k = segmentGran.bucketStart(new DateTime(r.getTimestampFromEpoch)).getMillis
            }
            k -> r.asInstanceOf[MapBasedInputRow].getEvent
          }
        )
      }
  }

  /**
    * Take a map of indices and size for that index, and return a map of (index, sub_index)->new_index
    * each new_index of which can have at most rowsPerPartition assuming random-ish hashing into the indices
    *
    * @param inMap            A map of index to count of items in that index
    * @param rowsPerPartition The maximum desired size per output index.
    * @return A map of (index, sub_index)->new_index . The size of this map times rowsPerPartition is greater than
    *         or equal to the number of events (sum of keys of inMap)
    */
  def getSizedPartitionMap(inMap: Map[Long, Long], rowsPerPartition: Long): Map[(Long, Long), Int] = inMap
    .filter(_._2 > 0)
    .map(
      (x) => {
        val dateRangeBucket = x._1
        val numEventsInRange = x._2
        Range
          .apply(0, (numEventsInRange / rowsPerPartition + 1).toInt)
          .map(a => (dateRangeBucket, a.toLong))
      }
    )
    .foldLeft(Seq[(Long, Long)]())(_ ++ _)
    .foldLeft(Map[(Long, Long), Int]())(
      (b: Map[(Long, Long), Int], v: (Long, Long)) => b + (v -> b.size)
    )

  def logInfo(str: String): Unit = {
    log.info(str, null)
  }

  def logTrace(str: String): Unit = {
    log.trace(str, null)
  }

  def logDebug(str: String): Unit = {
    log.debug(str, null)
  }

  def logError(str: String, t: Throwable): Unit = {
    log.error(t, str, null)
  }
}

object SerializedJsonStatic {
  val LOG = new Logger("org.apache.druid.indexer.spark.SerializedJsonStatic")
  val defaultService = "spark-indexer"
  // default indexing service port
  val defaultPort = "8090"
  val defaultTlsPort = "8290"
  lazy val injector: Injector = {
    try {
      Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), List[Module](
          new Module {
            override def configure(binder: Binder): Unit = {
              binder.bindConstant().annotatedWith(Names.named("serviceName")).to(defaultService)
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(defaultPort)
              binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(defaultTlsPort)
              JsonConfigProvider.bind(binder, "druid", classOf[DruidNode], classOf[Self])
            }
          }
        )
      )
    }
    catch {
      case NonFatal(e) =>
        LOG.error(e, "Error initializing injector")
        throw e
    }
  }
  // I would use ObjectMapper.copy().configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false)
  // But https://github.com/FasterXML/jackson-databind/issues/696 isn't until 2.5.1, and anything 2.5 or greater breaks
  // EVERYTHING
  // So instead we have to capture the close
  lazy val mapper: ObjectMapper = {
    try {
      injector.getInstance(Key.get(classOf[ObjectMapper], classOf[Json]))
    }
    catch {
      case NonFatal(e) =>
        LOG.error(e, "Error getting object mapper instance")
        throw e
    }
  }

  lazy val lifecycle: Lifecycle = {
    try {
      injector.getInstance(classOf[Lifecycle])
    } catch {
      case NonFatal(e) =>
        LOG.error(e, "Error getting life cycle instance")
        throw e
    }
  }

  lazy val emitter: ServiceEmitter = {
    try {
      injector.getInstance(classOf[ServiceEmitter])
    } catch {
      case NonFatal(e) =>
        LOG.error(e, "Error getting service emitter instance")
        throw e
    }
  }

  def captureCloseOutputStream(ostream: OutputStream): OutputStream =
    new FilterOutputStream(ostream) {
      override def close(): Unit = {
        // Ignore
      }
    }

  def captureCloseInputStream(istream: InputStream): InputStream = new FilterInputStream(istream) {
    override def close(): Unit = {
      // Ignore
    }
  }

  def getLog = LOG

  val mapTypeReference = new TypeReference[java.util.Map[String, Object]] {}
}

/**
  * This is tricky. The type enforcing is only done at compile time. The JSON serde plays it fast and
  * loose with the types
  */
@SerialVersionUID(713838456349L)
class SerializedJson[A](inputDelegate: A) extends KryoSerializable with Serializable {
  @transient var delegate: A = Option(inputDelegate).getOrElse(throw new NullPointerException())

  @throws[IOException]
  private def writeObject(output: ObjectOutputStream) = {
    innerWrite(output)
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    innerWrite(output)
  }

  def innerWrite(output: OutputStream): Unit = SerializedJsonStatic.mapper
    .writeValue(SerializedJsonStatic.captureCloseOutputStream(output), toJavaMap)

  def toJavaMap = mapAsJavaMap(
    Map(
      "class" -> delegate.getClass.getCanonicalName,
      "delegate" -> SerializedJsonStatic.mapper.writeValueAsString(delegate)
    )
  )

  def getMap(input: InputStream) = SerializedJsonStatic
    .mapper
    .readValue(
      SerializedJsonStatic.captureCloseInputStream(input),
      SerializedJsonStatic.mapTypeReference
    ).asInstanceOf[java.util.Map[String, Object]].toMap[String, Object]


  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readObject(input: ObjectInputStream) = {
    if (SerializedJsonStatic.getLog.isTraceEnabled) {
      SerializedJsonStatic.getLog
        .trace("Reading Object")
    }
    fillFromMap(getMap(input))
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    if (SerializedJsonStatic.getLog.isTraceEnabled) {
      SerializedJsonStatic.getLog
        .trace("Reading Kryo")
    }
    fillFromMap(getMap(input))
  }

  def fillFromMap(m: Map[String, Object]): Unit = {
    val clazzName: Class[_] = m.get("class") match {
      case Some(cn) => if (Thread.currentThread().getContextClassLoader == null) {
        if (SerializedJsonStatic.getLog.isTraceEnabled) {
          SerializedJsonStatic.getLog
            .trace(s"Using class's classloader [${getClass.getClassLoader}]")
        }
        getClass.getClassLoader.loadClass(cn.toString)
      } else {
        val classLoader = Thread.currentThread().getContextClassLoader
        if (SerializedJsonStatic.getLog.isTraceEnabled) {
          SerializedJsonStatic.getLog
            .trace(s"Using context classloader [$classLoader]")
        }
        classLoader.loadClass(cn.toString)
      }
      case _ => throw new NullPointerException("Missing `class`")
    }
    delegate = m.get("delegate") match {
      case Some(d) => SerializedJsonStatic.mapper.readValue(d.toString, clazzName)
        .asInstanceOf[A]
      case _ => throw new NullPointerException("Missing `delegate`")
    }
    if (SerializedJsonStatic.getLog.isTraceEnabled) {
      SerializedJsonStatic.getLog.trace(s"Read in $delegate")
    }
  }

  def getDelegate = delegate
}

@SerialVersionUID(68710585891L)
class SerializedHadoopConfig(delegate: Configuration) extends KryoSerializable with Serializable {
  @transient var del = delegate

  @throws[IOException]
  private def writeObject(out: ObjectOutputStream) = {
    if (SerializedJsonStatic.getLog.isTraceEnabled) SerializedJsonStatic.getLog
      .trace("Writing Hadoop Object")
    del.write(out)
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readObject(in: ObjectInputStream) = {
    if (SerializedJsonStatic.getLog.isTraceEnabled) SerializedJsonStatic.getLog
      .trace("Reading Hadoop Object")
    del = new Configuration()
    del.readFields(in)
  }

  def getDelegate = del

  override def write(kryo: Kryo, output: Output): Unit = {
    if (SerializedJsonStatic.getLog.isTraceEnabled) SerializedJsonStatic.getLog
      .trace("Writing Hadoop Kryo")
    writeObject(new ObjectOutputStream(output))
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    if (SerializedJsonStatic.getLog.isTraceEnabled) SerializedJsonStatic.getLog
      .trace("Reading Hadoop Kryo")
    readObject(new ObjectInputStream(input))
  }
}

class DateBucketPartitioner(@transient var gran: Granularity, intervals: Iterable[Interval]) extends Partitioner {
  val intervalMap: Map[Long, Int] = intervals
    .map(_.getStart.getMillis)
    .foldLeft(Map[Long, Int]())((a, b) => a + (b -> a.size))

  override def numPartitions: Int = intervalMap.size

  override def getPartition(key: Any): Int = key match {
    case null => throw new NullPointerException("Bad partition key")
    case (k: Long, v: Any) => getPartition(k)
    case (k: Long) =>
      val mapKey = gran.bucketStart(new DateTime(k)).getMillis
      val v = intervalMap.get(mapKey)
      if (v.isEmpty) {
        // Lazy ISE creation. getOrElse will create it each time
        throw new ISE(
          s"unknown bucket for datetime $mapKey. Known values are ${intervalMap.keys}"
        )
      }
      v.get
    case x => throw new IAE(s"Unknown type for $x")
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    val granularity = gran.asInstanceOf[PeriodGranularity]
    if (GranularityType.isStandard(gran)) {
      out.writeObject(new SerializedJson[String](granularity.getPeriod.toString))
    } else {
      out.writeObject(new SerializedJson[Granularity](granularity))
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    val value = in.readObject()
    gran = value match {
      case s: SerializedJson[_] =>
        s.delegate match {
          case str: String => new PeriodGranularity(new Period(str), null, null)
          case x : Granularity => x
        }
    }
  }
}

class DateBucketAndHashPartitioner(@transient var gran: Granularity,
                                   partMap: Map[(Long, Long), Int],
                                   optionalDims: Option[Set[String]] = None,
                                   optionalDimExclusions: Option[Set[String]] = None
                                  )
  extends Partitioner {
  lazy val shardLookups = partMap.groupBy { case ((bucket, _), _) => bucket }
    .map { case (bucket, indices) => bucket -> indices.size }
    .mapValues(maxPartitions => {
      val shardSpecs = (0 until maxPartitions)
        .map(new HashBasedNumberedShardSpec(_, maxPartitions, null, SerializedJsonStatic.mapper).asInstanceOf[ShardSpec])
      shardSpecs.head.getLookup(shardSpecs.asJava)
    })
  lazy val excludedDimensions = optionalDimExclusions.getOrElse(Set())

  override def numPartitions: Int = partMap.size

  override def getPartition(key: Any): Int = key match {
    case (k: Long, v: AnyRef) =>
      val eventMap: Map[String, AnyRef] = v match {
        case mm: util.Map[String, AnyRef] =>
          mm.asScala.toMap
        case mm: Map[String, AnyRef] =>
          mm
        case x =>
          throw new IAE(s"Unknown value type ${x.getClass} : [$x]")
      }
      val dateBucket = gran.bucketStart(new DateTime(k)).getMillis
      val shardLookup = shardLookups.get(dateBucket) match {
        case Some(sl) => sl
        case None => throw new IAE(s"Bad date bucket $dateBucket")
      }

      val shardSpec = shardLookup.getShardSpec(
        dateBucket,
        new MapBasedInputRow(
          dateBucket,
          optionalDims.getOrElse(eventMap.keySet -- excludedDimensions).toList.asJava, eventMap
        )
      )
      val timePartNum = shardSpec.getPartitionNum

      partMap.get((dateBucket.toLong, timePartNum)) match {
        case Some(part) => part
        case None => throw new ISE(s"bad date and partition combo: ($dateBucket, $timePartNum)")
      }
    case x => throw new IAE(s"Unknown type ${x.getClass} : [$x]")
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    val granularity = gran.asInstanceOf[PeriodGranularity]
    if (GranularityType.isStandard(gran)) {
      out.writeObject(new SerializedJson[String](granularity.getPeriod.toString))
    } else {
      out.writeObject(new SerializedJson[Granularity](granularity))
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    val value = in.readObject()
    gran = value match {
      case s: SerializedJson[_] =>
        s.delegate match {
          case str: String => new PeriodGranularity(new Period(str), null, null)
          case x : Granularity => x
        }
    }
  }
}

object StaticIndex {
  val INDEX_IO = new IndexIO(
    SerializedJsonStatic.mapper,
    new ColumnConfig {
      override def columnCacheSizeBytes(): Int = 1000000
    }
  )

  val INDEX_MERGER_V9 = new IndexMergerV9(
    SerializedJsonStatic.mapper,
    INDEX_IO,
    TmpFileSegmentWriteOutMediumFactory.instance()
  )
}
