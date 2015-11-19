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
import com.google.inject.{Binder, Key, Module}
import com.metamx.common.logger.Logger
import com.metamx.common.{Granularity, IAE, ISE}
import io.druid.data.input.impl._
import io.druid.data.input.{MapBasedInputRow, ProtoBufInputRowParser}
import io.druid.guice.annotations.{Self, Smile}
import io.druid.guice.{GuiceInjectors, JsonConfigProvider}
import io.druid.indexer.{HadoopyStringInputRowParser, JobHelper}
import io.druid.initialization.Initialization
import io.druid.query.aggregation.AggregatorFactory
import io.druid.segment._
import io.druid.segment.incremental.{IncrementalIndexSchema, OnheapIncrementalIndex}
import io.druid.segment.indexing.DataSchema
import io.druid.segment.loading.DataSegmentPusher
import io.druid.server.DruidNode
import io.druid.timeline.DataSegment
import io.druid.timeline.partition.{NoneShardSpec, NumberedShardSpec}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.util.Progressable
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, SparkContext}
import org.joda.time.{DateTime, Interval}

import scala.collection.JavaConversions._

object SparkDruidIndexer
{
  val log = new Logger(SparkDruidIndexer.getClass)

  def loadData(
    dataFiles: Seq[String],
    dataSchema: SerializedJson[DataSchema],
    ingestInterval: Interval,
    rowsPerPartition: Long,
    rowsPerPersist: Int,
    outPathString: String,
    indexSpec: IndexSpec,
    sc: SparkContext
  ): Seq[DataSegment] =
  {
    val dataSource = dataSchema.getDelegate.getDataSource
    log.info("Launching Spark task with jar version [%s]", getClass.getPackage.getImplementationVersion)
    val dataSegmentVersion = DateTime.now().toString
    val aggs: Array[SerializedJson[AggregatorFactory]] = dataSchema.getDelegate.getAggregators
      .map(x => new SerializedJson[AggregatorFactory](x))
    log.info("Starting caching of raw data for [%s] over interval [%s]", dataSource, ingestInterval)

    val baseData = sc.textFile(dataFiles mkString ",") // Hadoopify the data so it doesn't look so silly in Spark's DAG
      .mapPartitions(
        (it) => {
          val i = dataSchema.getDelegate.getParser match {
            case x: StringInputRowParser => it.map(x.parse)
            case x: HadoopyStringInputRowParser => it.map(x.parse)
            case x: ProtoBufInputRowParser => throw new
                UnsupportedOperationException("Cannot use Protobuf for text input")
            case x =>
              log
                .warn(
                  "%s",
                  "Could not figure out how to handle class [%s]. Hoping it can handle string input" format x.getClass
                )
              it.map(x.asInstanceOf[InputRowParser[Any]].parse)
          }
          i.filter(r => ingestInterval.contains(r.getTimestamp))
            .map(
              r => {
                val queryGran = dataSchema.getDelegate.getGranularitySpec.getQueryGranularity
                val segmentGran = dataSchema.getDelegate.getGranularitySpec.getSegmentGranularity
                var k: Long = queryGran.truncate(r.getTimestampFromEpoch)
                if (k < 0) {
                  // Example: AllGranularity
                  k = segmentGran.truncate(new DateTime(r.getTimestampFromEpoch)).getMillis
                }
                k -> r.asInstanceOf[MapBasedInputRow].getEvent
              }
            )
        },
        preservesPartitioning = false
      )
      .persist(StorageLevel.DISK_ONLY)

    log.info("Starting uniqes")
    val partitionMap: Map[Long, Long] = baseData
      .countApproxDistinctByKey(
        0.05,
        new DateBucketPartitioner(dataSchema.getDelegate.getGranularitySpec.getSegmentGranularity, ingestInterval)
      )
      .map(
        x => dataSchema.getDelegate.getGranularitySpec.getSegmentGranularity.truncate(new DateTime(x._1))
          .getMillis -> x._2
      )
      .reduceByKey(_ + _)
      .collect().toMap

    // Map key tuple is DateBucket, PartitionInBucket with map value of Partition #
    val hashToPartitionMap: Map[(Long, Long), Int] = getSizedPartitionMap(partitionMap, rowsPerPartition)

    // Get dimension values and dims per timestamp
    hashToPartitionMap.foreach {
      (a: ((Long, Long), Int)) => {
        log
          .info(
            "%s",
            "Date Bucket [%s] with partition number [%s] has total partition number [%s]" format
              (a._1._1, a._1._2, a._2)
          )
      }
    }

    val indexSpec_passable = new SerializedJson[IndexSpec](indexSpec)

    val partitioned_data = baseData
      .map(_ -> 0)
      .partitionBy(
        new DateBucketAndHashPartitioner(
          dataSchema.getDelegate.getGranularitySpec.getSegmentGranularity,
          ingestInterval,
          hashToPartitionMap
        )
      )
      .mapPartitionsWithIndex(
        (index, it) => {

          val partitionNums = hashToPartitionMap.filter(_._2 == index).map(_._1._2.toInt).toSeq
          if (partitionNums.isEmpty) {
            throw new ISE(
              "%s",
              "Partition [%s] not found in partition map. Valid parts: %s" format
                (index, hashToPartitionMap.values)
            )
          }
          if (partitionNums.length != 1) {
            throw new
                ISE("%s", "Too many partitions found for index [%s] Found %s" format(index, partitionNums))
          }
          val partitionNum = partitionNums.head
          val timeBucket = hashToPartitionMap.filter(_._2 == index).map(_._1._1).head
          val timeInterval = dataSchema.getDelegate.getGranularitySpec.getSegmentGranularity.getIterable(ingestInterval)
            .toSeq.filter(_.getStart.getMillis == timeBucket).head
          log.info("Creating index [%s] for date range [%s]" format(partitionNum, timeInterval))
          val partitionCount = hashToPartitionMap.count(_._1._1 == timeBucket)

          val parser: InputRowParser[_] = SerializedJsonStatic.mapper
            .convertValue(dataSchema.getDelegate.getParserMap, classOf[InputRowParser[_]])

          val rows = it

          val pusher: DataSegmentPusher = SerializedJsonStatic.injector.getInstance(classOf[DataSegmentPusher])
          val tmpPersistDir = Files.createTempDirectory("persist").toFile
          val tmpMergeDir = Files.createTempDirectory("merge").toFile
          val closer = Closer.create()
          closer.register(
            new Closeable
            {
              override def close(): Unit = {
                FileUtils.deleteDirectory(tmpPersistDir)
              }
            }
          )
          closer.register(
            new Closeable
            {
              override def close(): Unit = {
                FileUtils.deleteDirectory(tmpMergeDir)
              }
            }
          )
          try {
            // Only one SparkContext per JVM, and it should already be defined by the time we get to here
            val hadoopConf = SparkContext.getOrCreate().hadoopConfiguration
            val outPath = new Path(outPathString)
            val hadoopFs = outPath.getFileSystem(hadoopConf)
            val dimensions = dataSchema.getDelegate.getParser.getParseSpec.getDimensionsSpec.getDimensions

            val file = IndexMerger.merge(
              seqAsJavaList(
                rows
                  .grouped(rowsPerPersist)
                  .map(
                    _.foldLeft(
                      closer.register(
                        new OnheapIncrementalIndex(
                          new IncrementalIndexSchema.Builder()
                            .withDimensionsSpec(parser)
                            .withQueryGranularity(dataSchema.getDelegate.getGranularitySpec.getQueryGranularity)
                            .withMetrics(aggs.map(_.getDelegate))
                            .withMinTimestamp(timeBucket)
                            .build()
                          , rowsPerPersist
                        )
                      )
                    )(
                      (index: OnheapIncrementalIndex, r) => {
                        index.add(new MapBasedInputRow(r._1._1, dimensions, r._1._2))
                        index
                      }
                    )
                  ).map(
                  (incIndex: OnheapIncrementalIndex) => {
                    new QueryableIndexIndexableAdapter(
                      closer.register(
                        IndexIO.loadIndex(
                          IndexMerger
                            .persist(incIndex, timeInterval, tmpPersistDir, null, indexSpec_passable.getDelegate)
                        )
                      )
                    )
                  }
                ).toSeq
              ),
              aggs.map(_.getDelegate),
              tmpMergeDir,
              null,
              indexSpec_passable.getDelegate,
              new ProgressIndicator
              {
                override def stop(): Unit = {
                  log.trace("Stop")
                }

                override def stopSection(s: String): Unit = {
                  if (log.isTraceEnabled) log.trace("%s", "Stop [%s]" format s)
                }

                override def progress(): Unit = {
                  log.trace("Progress")
                }

                override def startSection(s: String): Unit = {
                  if (log.isTraceEnabled) log.trace("%s", "Start [%s]" format s)
                }

                override def progressSection(s: String, s1: String): Unit = {
                  if (log.isTraceEnabled) log.trace("%s", "Start [%s]:[%s]" format(s, s1))
                }

                override def start(): Unit = {
                  log.trace("Start")
                }
              }
            )
            val dataSegment = JobHelper.serializeOutIndex(
              new DataSegment(
                dataSource,
                timeInterval,
                dataSegmentVersion,
                null,
                dimensions,
                aggs.map(_.getDelegate.getName).toList,
                if (partitionCount == 1) {
                  new NoneShardSpec()
                } else {
                  new NumberedShardSpec(partitionNum, partitionCount)
                },
                -1,
                -1
              ),
              hadoopConf,
              new Progressable
              {
                override def progress(): Unit = log.debug("Progress")
              },
              new TaskAttemptID(new org.apache.hadoop.mapred.TaskID(), index),
              file,
              JobHelper.makeSegmentOutputPath(
                outPath,
                hadoopFs,
                dataSource,
                dataSegmentVersion,
                timeInterval,
                index
              )
            )
            val finalDataSegment = pusher.push(file, dataSegment)
            log.info("Finished pushing [%s]", finalDataSegment)
            Seq(new SerializedJson[DataSegment](finalDataSegment)).iterator
          }
          finally {
            closer.close()
          }
        }
      )
    val results = partitioned_data.cache().collect().map(_.getDelegate)
    log.info("Finished with %s", util.Arrays.deepToString(results.map(_.toString)))
    results.toSeq
  }

  /**
    * Take a map of indices and size for that index, and return a map of (index, sub_index)->new_index
    * each new_index of which can have at most rowsPerPartition assuming random-ish hashing into the indices
    * @param inMap A map of index to count of items in that index
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
    .foldLeft(Map[(Long, Long), Int]())((b: Map[(Long, Long), Int], v: (Long, Long)) => b + (v -> b.size))
}

object SerializedJsonStatic
{
  val log: Logger = new Logger(classOf[SerializedJson[Any]])
  val injector    = Initialization.makeInjectorWithModules(
    GuiceInjectors.makeStartupInjector(), List[Module](
      new Module
      {
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
  // I would use ObjectMapper.copy().configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false)
  // But https://github.com/FasterXML/jackson-databind/issues/696 isn't until 4.5.1, and anything 4.5 or greater breaks
  // EVERYTHING
  // So instead we have to capture the close
  val mapper      = injector.getInstance(Key.get(classOf[ObjectMapper], classOf[Smile]))

  def captureCloseOutputStream(ostream: OutputStream): OutputStream =
    new FilterOutputStream(ostream)
    {
      override def close(): Unit = {
        // Ignore
      }
    }

  def captureCloseInputStream(istream: InputStream): InputStream = new FilterInputStream(istream) {
    override def close(): Unit = {
      // Ignore
    }
  }

  val mapTypeReference = new TypeReference[java.util.Map[String, Object]] {}
}

/**
  * This is tricky. The type enforcing is only done at compile time. The JSON serde plays it fast and loose with the types
  */
@SerialVersionUID(713838456349L)
class SerializedJson[A](inputDelegate: A) extends KryoSerializable with Serializable
{
  @transient var delegate: A = inputDelegate

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
      "class" -> inputDelegate.getClass.getCanonicalName,
      "delegate" -> SerializedJsonStatic.mapper.writeValueAsString(delegate)
    )
  )

  def getMap(input: InputStream) = SerializedJsonStatic
    .mapper
    .readValue(SerializedJsonStatic.captureCloseInputStream(input), SerializedJsonStatic.mapTypeReference)
    .asInstanceOf[java.util.Map[String, Object]].toMap[String, Object]


  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readObject(input: ObjectInputStream) = {
    SerializedJsonStatic.log.trace("Reading Object")
    fillFromMap(getMap(input))
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    SerializedJsonStatic.log.trace("Reading Kryo")
    fillFromMap(getMap(input))
  }

  def fillFromMap(m: Map[String, Object]): Unit = {
    val clazzName: Class[_] = m.get("class") match {
      case Some(cn) => if (Thread.currentThread().getContextClassLoader == null) {
        SerializedJsonStatic.log.trace("Using class's classloader [%s]", getClass.getClassLoader)
        getClass.getClassLoader.loadClass(cn.toString)
      } else {
        SerializedJsonStatic.log.trace("Using context classloader [%s]", Thread.currentThread().getContextClassLoader)
        Thread.currentThread().getContextClassLoader.loadClass(cn.toString)
      }
      case _ => throw new NullPointerException("Missing `class`")
    }
    delegate = m.get("delegate") match {
      case Some(d) => SerializedJsonStatic.mapper.readValue(d.toString, clazzName).asInstanceOf[A]
      case _ => throw new NullPointerException("Missing `delegate`")
    }
    if (SerializedJsonStatic.log.isTraceEnabled) {
      SerializedJsonStatic.log.trace("Read in %s", delegate.toString)
    }
  }

  def getDelegate = delegate
}

class DateBucketPartitioner(gran: Granularity, interval: Interval) extends Partitioner
{
  val intervalMap: Map[Long, Int] = gran.getIterable(interval)
    .toSeq
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
          "%s",
          "unknown bucket for datetime %s. Known values are %s" format(mapKey, intervalMap.keys)
        )
      }
      v.get
    case x => throw new IAE("%s", "Unknown type for %s" format x)
  }
}

class DateBucketAndHashPartitioner(gran: Granularity, interval: Interval, partMap: Map[(Long, Long), Int])
  extends Partitioner
{
  val maxTimePerBucket = partMap.groupBy(_._1._1).map(e => e._1 -> e._2.size)

  override def numPartitions: Int = partMap.size

  override def getPartition(key: Any): Int = key match {
    case (k: Long, v: AnyRef) =>
      val dateBucket = gran.truncate(new DateTime(k)).getMillis
      val modSize = maxTimePerBucket.get(dateBucket.toLong)
      if (modSize.isEmpty) {
        // Lazy ISE creation
        throw new ISE("%s", "bad date bucket [%s]. available: %s" format(dateBucket.toLong, maxTimePerBucket.keySet))
      }
      val hash = Math.abs(v.hashCode()) % modSize.get
      partMap
        .getOrElse(
          (dateBucket.toLong, hash.toLong), {
            throw new ISE("bad hash and bucket combo: (%s, %s)" format(dateBucket, hash))
          }
        )
    case x => throw new IAE("%s", "Unknown type [%s]" format x)
  }
}
