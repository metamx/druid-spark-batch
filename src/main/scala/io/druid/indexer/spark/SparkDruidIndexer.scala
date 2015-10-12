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
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.io.Closer
import com.google.inject.{Binder, Key, Module}
import com.metamx.common.logger.Logger
import io.druid.data.input.MapBasedInputRow
import io.druid.data.input.impl._
import io.druid.granularity.QueryGranularity
import io.druid.guice.annotations.{Self, Smile}
import io.druid.guice.{GuiceInjectors, JsonConfigProvider}
import io.druid.indexer.JobHelper
import io.druid.initialization.Initialization
import io.druid.query.aggregation.AggregatorFactory
import io.druid.segment._
import io.druid.segment.incremental.{IncrementalIndexSchema, OnheapIncrementalIndex}
import io.druid.segment.loading.DataSegmentPusher
import io.druid.server.DruidNode
import io.druid.timeline.DataSegment
import io.druid.timeline.partition.NumberedShardSpec
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.util.Progressable
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.joda.time.{DateTime, Interval}

import scala.collection.JavaConversions._

object SparkDruidIndexer
{
  val log = new Logger(SparkDruidIndexer.getClass)

  def loadData(
    data_file: Seq[String],
    dataSource: String,
    parseSpec: SerializedJson[ParseSpec],
    ingestInterval: Interval,
    aggregatorFactories: Seq[AggregatorFactory],
    rowsPerPartition: Long,
    rowsPerPersist: Int,
    outPathString: String,
    queryGran: QueryGranularity,
    sc: SparkContext
    ): Seq[DataSegment] =
  {
    log.info("Launching Spark task with jar version [%s]", getClass.getPackage.getImplementationVersion)
    val dataSegmentVersion = DateTime.now().toString
    val hadoopConfig = new SerializedHadoopConfig(sc.hadoopConfiguration)
    val aggs: Array[SerializedJson[AggregatorFactory]] = aggregatorFactories
      .map(x => new SerializedJson[AggregatorFactory](x))
      .toArray
    log.info("Starting caching of raw data for [%s] over interval [%s]", dataSource, ingestInterval)
    val passableGran = new SerializedJson[QueryGranularity](queryGran)

    val baseData = sc
      .union(data_file.map(sc.textFile(_)))
      .mapPartitions(
        (it) => {
          val parser = parseSpec.getDelegate.makeParser()
          val mapParser = new MapInputRowParser(parseSpec.getDelegate)
          val row = it.map(parser.parse)
          row
            .map(r => mapParser.parse(r).asInstanceOf[MapBasedInputRow])
            .filter(r => ingestInterval.contains(r.getTimestamp))
            .map(
              r => {
                passableGran.getDelegate.truncate(r.getTimestampFromEpoch) -> getDimValues(r)
              }
            )
        }
      )
      .persist(StorageLevel.MEMORY_AND_DISK)

    log.info("Starting uniqes")
    val uniques = baseData.countApproxDistinct()
    // Get dimension values and dims per timestamp

    val numParts = uniques / rowsPerPartition + 1
    log.info("Found %s unique values. Breaking into %s partitions", uniques.toString, numParts.toString)

    val partitioned_data = baseData
      .repartition(numParts.toInt)
      .mapPartitionsWithIndex(
        (index, it) => {
          val rows = it
            .map(r => new MapBasedInputRow(r._1, parseSpec.getDelegate.getDimensionsSpec.getDimensions, r._2.toMap))

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
            // Fail early if hadoop config is screwed up
            val hadoopConf = hadoopConfig.getDelegate
            val outPath = new Path(outPathString)
            val hadoopFs = outPath.getFileSystem(hadoopConf)

            val file = IndexMerger.merge(
              seqAsJavaList(
                rows
                  .grouped(rowsPerPersist)
                  .map(
                    _.foldLeft(
                      new OnheapIncrementalIndex(
                        new IncrementalIndexSchema.Builder()
                          .withDimensionsSpec(parseSpec.getDelegate.getDimensionsSpec)
                          .withQueryGranularity(passableGran.getDelegate)
                          .withMetrics(aggs.map(_.getDelegate))
                          .build()
                        , rowsPerPersist
                      )
                    )(
                      (index: OnheapIncrementalIndex, r) => {
                        index.add(index.formatRow(r))
                        index
                      }
                    )
                  ).map(
                  (incIndex: OnheapIncrementalIndex) => {
                    new QueryableIndexIndexableAdapter(
                      closer.register(
                        IndexIO.loadIndex(
                          IndexMerger.persist(incIndex, tmpPersistDir, null, new IndexSpec())
                        )
                      )
                    )
                  }
                ).toSeq
              ),
              aggs.map(_.getDelegate),
              tmpMergeDir,
              null,
              new IndexSpec(),
              new ProgressIndicator
              {
                override def stop(): Unit = {
                  log.trace("Stop")
                }

                override def stopSection(s: String): Unit = {
                  log.trace("Stop [%s]", s)
                }

                override def progress(): Unit = {
                  log.trace("Progress")
                }

                override def startSection(s: String): Unit = {
                  log.trace("Start [%s]", s)
                }

                override def progressSection(s: String, s1: String): Unit = {
                  log.trace("Start [%s]:[%s]", s, s1)
                }

                override def start(): Unit = {
                  log.trace("Start")
                }
              }
            )
            val dataSegment = JobHelper.serializeOutIndex(
              new DataSegment(
                dataSource,
                ingestInterval,
                dataSegmentVersion,
                null,
                parseSpec.getDelegate.getDimensionsSpec.getDimensions,
                aggs.map(_.getDelegate.getName).toList,
                new NumberedShardSpec(index, numParts.toInt),
                -1,
                -1
              ),
              hadoopConf,
              new Progressable
              {
                override def progress(): Unit = { log.debug("Progress") }
              },
              new TaskAttemptID(new org.apache.hadoop.mapred.TaskID(), index),
              file,
              JobHelper.makeSegmentOutputPath(
                outPath,
                hadoopFs,
                dataSource,
                dataSegmentVersion,
                ingestInterval,
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

  def getDimValues(r: MapBasedInputRow) = {
    collectionAsScalaIterable(r.getDimensions)
      .toSet
      .map((s: String) => s -> collectionAsScalaIterable(r.getDimension(s)).toSet)
  }
}

object SerializedJsonStatic
{
  val log: Logger      = new Logger(classOf[SerializedJson[Any]])
  val injector         = Initialization.makeInjectorWithModules(
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
  val mapper           = injector.getInstance(Key.get(classOf[ObjectMapper], classOf[Smile]))
    .copy()
    .configure(
      JsonParser.Feature.AUTO_CLOSE_SOURCE,
      false
    )
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

  def innerWrite(output: OutputStream): Unit = {
    val m = toJavaMap
    val b = SerializedJsonStatic.mapper.writeValueAsBytes(m)
    if (SerializedJsonStatic.log.isTraceEnabled) {
      SerializedJsonStatic.log.trace("Writing [%s] in %s bytes", delegate.toString, new Integer(b.length))
    }
    output.write(b)
  }

  def toJavaMap = mapAsJavaMap(
    Map(
      "class" -> inputDelegate.getClass.getCanonicalName,
      "delegate" -> SerializedJsonStatic.mapper.writeValueAsString(delegate)
    )
  )

  def getMap(input: InputStream) = SerializedJsonStatic
    .mapper
    .readValue(input, SerializedJsonStatic.mapTypeReference)
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

@SerialVersionUID(68710585891L)
class SerializedHadoopConfig(delegate: Configuration) extends KryoSerializable with Serializable
{
  @transient var del = delegate

  @throws[IOException]
  private def writeObject(out: ObjectOutputStream) = {
    SerializedJsonStatic.log.trace("Writing Hadoop Object")
    del.write(out)
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readObject(in: ObjectInputStream) = {
    SerializedJsonStatic.log.trace("Reading Hadoop Object")
    del = new Configuration()
    del.readFields(in)
  }

  def getDelegate = del

  override def write(kryo: Kryo, output: Output): Unit = {
    SerializedJsonStatic.log.trace("Writing Hadoop Kryo")
    writeObject(new ObjectOutputStream(output))
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    SerializedJsonStatic.log.trace("Reading Hadoop Kryo")
    readObject(new ObjectInputStream(input))
  }
}

/*
@SerialVersionUID(978137489L)
class SerializedMapBasedInputRow(inputDelegate: MapBasedInputRow) extends KryoSerializable with Serializable
{
  @transient var delegate: MapBasedInputRow = inputDelegate

  private def innerWrite(output: OutputStream): Unit = {
    val m = mapAsJavaMap(
      Map(
        "timestamp" -> getDelegate.getTimestampFromEpoch,
        "dimensions" -> getDelegate.getDimensions,
        "event" -> getDelegate.getEvent
      )
    )
    val b = SerializedJsonStatic.mapper.writeValueAsBytes(m)
    if (SerializedJsonStatic.log.isTraceEnabled) {
      SerializedJsonStatic.log.trace("Writing %s in %s bytes", delegate.toString, new Integer(b.length))
    }
    output.write(b)
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    SerializedJsonStatic.log.trace("Writing MapInputRow Kryo")
    innerWrite(output)
  }

  @throws[IOException]
  private def writeObject(output: ObjectOutputStream) = {
    SerializedJsonStatic.log.trace("Writing MapInputRow Object")
    innerWrite(output)
  }

  private def innerRead(input: InputStream): Unit = {
    val m: Map[String, Object] = SerializedJsonStatic
      .mapper
      .readValue(input, new TypeReference[java.util.Map[String, Object]] {})
      .asInstanceOf[java.util.Map[String, Object]].toMap
    val timestamp: Long = m.get("timestamp") match {
      case Some(l) => l.asInstanceOf[Long]
      case _ => throw new NullPointerException("Missing `timestamp`")
    }
    val dimensions: util.List[String] = m.get("dimensions") match {
      case Some(d) => d.asInstanceOf[util.List[String]]
      case _ => throw new NullPointerException("Missing `dimensions`")
    }
    val event: util.Map[String, AnyRef] = m.get("event") match {
      case Some(e) => e.asInstanceOf[util.Map[String, AnyRef]]
      case _ => throw new NullPointerException("Missing `event`")
    }
    delegate = new MapBasedInputRow(timestamp, dimensions, event)
    if (SerializedJsonStatic.log.isTraceEnabled) {
      SerializedJsonStatic.log.trace("Read in %s", delegate)
    }
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    SerializedJsonStatic.log.trace("Reading MapInputRow Kryo")
    innerRead(input)
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readObject(input: ObjectInputStream) = {
    SerializedJsonStatic.log.trace("Reading MapInputRow Object")
    innerRead(input)
  }

  override def hashCode(): Int =
  {
    getDimensionValues.hashCode()
  }

  def getDelegate = delegate

  def getDimensionValues = collectionAsScalaIterable(getDelegate.getDimensions)
    .toSet
    .map((s: String) => s -> collectionAsScalaIterable(getDelegate.getDimension(s)).toSet)
}
*/
