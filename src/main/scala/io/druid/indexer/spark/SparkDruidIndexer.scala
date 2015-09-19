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

import java.io.{Closeable, IOException, ObjectInputStream, ObjectOutputStream}
import java.nio.file.Files
import java.util

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
    val dataSegmentVersion = DateTime.now().toString
    val hadoopConfig = new SerializedHadoopConfig(sc.hadoopConfiguration)
    val aggs: Array[SerializedJson[AggregatorFactory]] = aggregatorFactories
      .map(x => new SerializedJson[AggregatorFactory](x))
      .toArray
    log.info("Starting caching of raw data for [%s] over interval [%s]", dataSource, ingestInterval)
    val passableGran = new SerializedJson[QueryGranularity](queryGran)
    val raw_data = sc
      .union(data_file.map(sc.textFile(_)))
      .mapPartitionsWithIndex(
        (index, it) => {
          val parser = parseSpec.getDelegate.makeParser()
          val mapParser = new MapInputRowParser(parseSpec.getDelegate)
          val row = it.map(parser.parse)
          row
            .map(r => mapParser.parse(r).asInstanceOf[MapBasedInputRow])
            .map(r => new SerializedMapBasedInputRow(r))
        }
      )
      .filter(r => ingestInterval.contains(r.getDelegate.getTimestamp))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    // Get dimension values and dims per timestamp

    log.info("Defining uniques mapping")
    val data_dims = raw_data
      .map(
        r => {
          passableGran.getDelegate.truncate(r.getDelegate.getTimestampFromEpoch) -> r.getDimensionValues
        }
      )
    log.info("Starting uniqes")
    val uniques = data_dims.countApproxDistinct()
    val numParts = uniques / rowsPerPartition + 1
    log.info("Found %s unique values. Breaking into %s partitions", uniques.toString, numParts.toString)

    val partitioned_data = raw_data
      .repartition(numParts.toInt)
      .mapPartitionsWithIndex(
        (index, rows) => {
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
                          index.add(index.formatRow(r.getDelegate))
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
              new LoggingProgressIndicator("index-merge")
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
    val results = partitioned_data.collect().map(_.getDelegate)
    log.info("Finished with %s", util.Arrays.deepToString(results.map(_.toString)))
    results.toSeq
  }
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
  val mapper      = injector.getInstance(Key.get(classOf[ObjectMapper], classOf[Smile]))
    .copy()
    .configure(
      JsonParser.Feature.AUTO_CLOSE_SOURCE,
      false
    )
}

/**
 * This is tricky. The type enforcing is only done at compile time. The JSON serde plays it fast and loose with the types
 */
@SerialVersionUID(713838456349L)
class SerializedJson[A](inputDelegate: A) extends Serializable
{
  @transient var delegate: A = inputDelegate

  @throws[IOException]
  private def writeObject(out: ObjectOutputStream) = {
    val m = mapAsJavaMap(
      Map(
        "class" -> inputDelegate.getClass.getCanonicalName,
        "delegate" -> SerializedJsonStatic.mapper.writeValueAsString(delegate)
      )
    )
    if (SerializedJsonStatic.log.isDebugEnabled) {
      SerializedJsonStatic.log.debug("Writing %s", delegate.toString)
    }
    out.write(SerializedJsonStatic.mapper.writeValueAsBytes(m))
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readObject(in: ObjectInputStream) = {
    val m: Map[String, Object] = SerializedJsonStatic
      .mapper
      .readValue(in, new TypeReference[java.util.Map[String, Object]] {})
      .asInstanceOf[java.util.Map[String, Object]].toMap
    val clazzName: Class[_] = m.get("class") match {
      case Some(cn) => if (Thread.currentThread().getContextClassLoader == null) {
        SerializedJsonStatic.log.debug("Using class's classloader [%s]", getClass.getClassLoader)
        getClass.getClassLoader.loadClass(cn.toString)
      } else {
        SerializedJsonStatic.log.debug("Using context classloader [%s]", Thread.currentThread().getContextClassLoader)
        Thread.currentThread().getContextClassLoader.loadClass(cn.toString)
      }
      case _ => throw new NullPointerException("Missing `class`")
    }
    delegate = m.get("delegate") match {
      case Some(d) => SerializedJsonStatic.mapper.readValue(d.toString, clazzName).asInstanceOf[A]
      case _ => throw new NullPointerException("Missing `delegate`")
    }
    if (SerializedJsonStatic.log.isDebugEnabled) {
      SerializedJsonStatic.log.debug("Read in %s", delegate.toString)
    }
  }

  def getDelegate = delegate
}

@SerialVersionUID(68710585891L)
class SerializedHadoopConfig(delegate: Configuration) extends Serializable
{
  var del = delegate

  def getDelegate = del

  @throws[IOException]
  private def writeObject(out: ObjectOutputStream) = {
    del.write(out)
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readObject(in: ObjectInputStream) = {
    del = new Configuration()
    del.readFields(in)
  }
}

@SerialVersionUID(978137489L)
class SerializedMapBasedInputRow(inputDelegate: MapBasedInputRow) extends Serializable
{
  @transient var delegate: MapBasedInputRow = inputDelegate

  @throws[IOException]
  private def writeObject(out: ObjectOutputStream) = {
    val m = mapAsJavaMap(
      Map(
        "timestamp" -> getDelegate.getTimestampFromEpoch,
        "dimensions" -> getDelegate.getDimensions,
        "event" -> getDelegate.getEvent
      )
    )
    if (SerializedJsonStatic.log.isDebugEnabled) {
      SerializedJsonStatic.log.debug("Writing %s", delegate.toString)
    }
    out.write(SerializedJsonStatic.mapper.writeValueAsBytes(m))
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readObject(in: ObjectInputStream) = {
    val m: Map[String, Object] = SerializedJsonStatic
      .mapper
      .readValue(in, new TypeReference[java.util.Map[String, Object]] {})
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
    if (SerializedJsonStatic.log.isDebugEnabled) {
      SerializedJsonStatic.log.debug("Read in %s", delegate)
    }
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