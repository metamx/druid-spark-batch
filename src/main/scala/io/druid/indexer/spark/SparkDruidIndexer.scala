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

import java.io._
import java.nio.file.Files
import java.util

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.io.Closer
import com.google.inject.{Binder, Injector, Key, Module}
import com.metamx.common.logger.Logger
import com.metamx.common.{Granularity, IAE, ISE}
import io.druid.data.input.impl._
import io.druid.data.input.{MapBasedInputRow, ProtoBufInputRowParser}
import io.druid.guice.annotations.{Json, Self}
import io.druid.guice.{GuiceInjectors, JsonConfigProvider}
import io.druid.indexer.{HadoopyStringInputRowParser, JobHelper}
import io.druid.initialization.Initialization
import io.druid.query.aggregation.AggregatorFactory
import io.druid.segment._
import io.druid.segment.column.ColumnConfig
import io.druid.segment.incremental.{IncrementalIndexSchema, OnheapIncrementalIndex}
import io.druid.segment.indexing.DataSchema
import io.druid.segment.loading.DataSegmentPusher
import io.druid.server.DruidNode
import io.druid.timeline.DataSegment
import io.druid.timeline.partition.{HashBasedNumberedShardSpec, NoneShardSpec, ShardSpec}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.util.Progressable
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, SparkContext}
import org.joda.time.{DateTime, Interval}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object SparkDruidIndexer {
  private val log = new Logger(getClass)
  def loadData(
                dataFiles: Seq[String],
                dataSchema: SerializedJson[DataSchema],
                ingestIntervals: Iterable[Interval],
                rowsPerPartition: Long,
                rowsPerPersist: Int,
                outPathString: String,
                indexSpec: IndexSpec,
                sc: SparkContext
              ): Seq[DataSegment] = {
    val dataSource = dataSchema.getDelegate.getDataSource
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
    val baseData = sc.textFile(dataFiles mkString ",")
      // Input data is probably in gigantic files, so redistribute
      .filter(
      s => {
        val row = dataSchema.getDelegate.getParser match {
          case x: StringInputRowParser => x.parse(s)
          case x: HadoopyStringInputRowParser => x.parse(s)
          case x: ProtoBufInputRowParser => throw new
              UnsupportedOperationException("Cannot use Protobuf for text input")
          case x =>
            logTrace(
              "Could not figure out how to handle class " +
                s"[${x.getClass.getCanonicalName}]. " +
                "Hoping it can handle string input"
            )
            x.asInstanceOf[InputRowParser[Any]].parse(s)
        }
        passableIntervals.exists(_.contains(row.getTimestamp))
      }
    )
      .repartition(startingPartitions)
      // Persist the strings only rather than the event map
      // We have to do the parsing twice this way, but serde of the map is killer as well
      .persist(StorageLevel.DISK_ONLY)
      .mapPartitions(
        it => {
          val i = dataSchema.getDelegate.getParser match {
            case x: StringInputRowParser => it.map(x.parse)
            case x: HadoopyStringInputRowParser => it.map(x.parse)
            case x: ProtoBufInputRowParser => throw new
                UnsupportedOperationException("Cannot use Protobuf for text input")
            case x =>
              logTrace(
                "Could not figure out how to handle class " +
                  s"[${x.getClass.getCanonicalName}]. " +
                  "Hoping it can handle string input"
              )
              it.map(x.asInstanceOf[InputRowParser[Any]].parse)
          }
          val queryGran = dataSchema.getDelegate.getGranularitySpec.getQueryGranularity
          val segmentGran = dataSchema.getDelegate.getGranularitySpec.getSegmentGranularity
          i.map(
            r => {
              var k: Long = queryGran.truncate(r.getTimestampFromEpoch)
              if (k < 0) {
                // Example: AllGranularity
                k = segmentGran.truncate(new DateTime(r.getTimestampFromEpoch)).getMillis
              }
              k -> r.asInstanceOf[MapBasedInputRow].getEvent
            }
          )
        }
      )

    logInfo("Starting uniqes")
    val optionalDims: Option[Set[String]] = if (dataSchema.getDelegate.getParser.getParseSpec.getDimensionsSpec.hasCustomDimensions) {
      val parseSpec = dataSchema.getDelegate.getParser.getParseSpec
      Some(parseSpec.getDimensionsSpec.getDimensionNames.asScala.toSet)
    } else {
      None
    }

    val uniquesEvents: RDD[(Long, util.Map[String, AnyRef])] = optionalDims match {
      case Some(dims) =>baseData.mapValues(_.filterKeys(dims.contains))
      case None => baseData
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
        .truncate(new DateTime(x._1))
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
            s" has total partition number [${ a._2}]"
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
            val hadoopFs = outPath.getFileSystem(hadoopConf)
            val dimSpec = dataSchema.getDelegate.getParser.getParseSpec.getDimensionsSpec
            val excludedDims = dimSpec.getDimensionExclusions
            val finalDims: Option[util.List[String]] = if (dimSpec.hasCustomDimensions) Some(dimSpec.getDimensionNames.asScala) else None

            val indices: util.List[IndexableAdapter] = rows.grouped(rowsPerPersist)
              .map(
                _.foldLeft(
                  new OnheapIncrementalIndex(
                    new IncrementalIndexSchema.Builder()
                      .withDimensionsSpec(parser)
                      .withQueryGranularity(
                        dataSchema.getDelegate
                          .getGranularitySpec.getQueryGranularity
                      )
                      .withMetrics(aggs.map(_.getDelegate))
                      .withMinTimestamp(timeBucket)
                      .build(),
                    true, // Throws exception on parse error
                    rowsPerPersist
                  )
                )(
                  (index: OnheapIncrementalIndex, r) => {
                    index.add(
                      index.formatRow(
                        new MapBasedInputRow(
                          r._1._1,
                          finalDims.getOrElse((r._1._2.keySet() -- excludedDims).toList.asJava),
                          r._1._2
                        )
                      )
                    )
                    index
                  }
                )
              ).map(
              (incIndex: OnheapIncrementalIndex) => {
                val adapter = new QueryableIndexIndexableAdapter(
                  closer.register(
                    StaticIndex.INDEX_IO.loadIndex(
                      StaticIndex.INDEX_MERGER
                        .persist(
                          incIndex,
                          timeInterval,
                          tmpPersistDir,
                          indexSpec_passable.getDelegate
                        )
                    )
                  )
                )
                // TODO: figure out a guaranteed way to close OnheapIncrementalIndex
                // without holding a hard reference in closer
                // It doesn't actually close anything FOR NOW
                // But will need to close for future proofing
                incIndex.close()
                adapter
              }
            ).toList
            val file = StaticIndex.INDEX_MERGER.merge(
              indices,
              aggs.map(_.getDelegate),
              tmpMergeDir,
              indexSpec_passable.getDelegate,
              new ProgressIndicator {
                override def stop(): Unit = {
                  logTrace("Stop")
                }

                override def stopSection(s: String): Unit = {
                  logTrace(s"Stop [$s]")
                }

                override def progress(): Unit = {
                  logTrace("Progress")
                }

                override def startSection(s: String): Unit = {
                  logTrace(s"Start [$s]")
                }

                override def progressSection(s: String, s1: String): Unit = {
                  logTrace(s"Progress [$s]:[$s1]")
                }

                override def start(): Unit = {
                  logTrace("Start")
                }
              }
            )
            val allDimensions: util.List[String] = indices
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
            val dataSegment = JobHelper.serializeOutIndex(
              dataSegmentTemplate,
              hadoopConf,
              new Progressable {
                override def progress(): Unit = logDebug("Progress")
              },
              new TaskAttemptID(new org.apache.hadoop.mapred.TaskID(), index),
              file,
              JobHelper.makeSegmentOutputPath(
                outPath,
                hadoopFs,
                dataSegmentTemplate
              )
            )
            val finalDataSegment = pusher.push(file, dataSegment)
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
  val LOG = new Logger("io.druid.indexer.spark.SerializedJsonStatic")
  lazy val injector: Injector = {
    try {
      Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), List[Module](
          new Module {
            override def configure(binder: Binder): Unit = {
              JsonConfigProvider
                .bindInstance(
                  binder,
                  Key.get(classOf[DruidNode], classOf[Self]),
                  new DruidNode("spark-indexer", null, null)
                )
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

class DateBucketPartitioner(gran: Granularity, intervals: Iterable[Interval]) extends Partitioner {
  val intervalMap: Map[Long, Int] = intervals
    .map(_.getStart.getMillis)
    .foldLeft(Map[Long, Int]())((a, b) => a + (b -> a.size))

  override def numPartitions: Int = intervalMap.size

  override def getPartition(key: Any): Int = key match {
    case null => throw new NullPointerException("Bad partition key")
    case (k: Long, v: Any) => getPartition(k)
    case (k: Long) =>
      val mapKey = gran.bucket(new DateTime(k)).getStart.getMillis
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
}

class DateBucketAndHashPartitioner(gran: Granularity,
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
      val dateBucket = gran.truncate(new DateTime(k)).getMillis
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
}

object StaticIndex {
  val INDEX_IO = new IndexIO(SerializedJsonStatic.mapper, new ColumnConfig {
    override def columnCacheSizeBytes(): Int = 1000000
  })
  val INDEX_MERGER = new IndexMerger(SerializedJsonStatic.mapper, INDEX_IO)
}
