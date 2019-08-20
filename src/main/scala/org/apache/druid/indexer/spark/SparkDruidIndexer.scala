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

import java.io.Closeable
import java.nio.file.Files
import java.util

import com.google.common.io.Closer
import org.apache.commons.io.FileUtils
import org.apache.druid.data.input.MapBasedInputRow
import org.apache.druid.data.input.impl._
import org.apache.druid.java.util.common.ISE
import org.apache.druid.java.util.common.logger.Logger
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent
import org.apache.druid.query.aggregation.AggregatorFactory
import org.apache.druid.segment._
import org.apache.druid.segment.incremental.{IncrementalIndex, IncrementalIndexSchema}
import org.apache.druid.segment.indexing.DataSchema
import org.apache.druid.segment.loading.DataSegmentPusher
import org.apache.druid.timeline.DataSegment
import org.apache.druid.timeline.partition.{HashBasedNumberedShardSpec, NumberedShardSpec}
import org.apache.hadoop.fs.Path
import org.apache.spark.scheduler.{AccumulableInfo, SparkListener, SparkListenerApplicationEnd, SparkListenerStageCompleted}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.joda.time.{DateTime, Interval}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

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
      buildV9Directly: Boolean,
      sparkSession: SparkSession
  ): Seq[DataSegment] = {
    val dataSource = dataSchema.getDelegate.getDataSource

    logInfo("Initializing emitter for metrics")
    registerSparkListeners(sparkSession, dataSource, ingestIntervals)

    val dataSegmentVersion = DateTime.now().toString
    val hadoopConfig = new SerializedHadoopConfig(sparkSession.sparkContext.hadoopConfiguration)
    val aggs: Array[SerializedJson[AggregatorFactory]] = dataSchema.getDelegate.getAggregators
      .map(x => new SerializedJson[AggregatorFactory](x))
    val passableIntervals = ingestIntervals.foldLeft(Set[Interval]())((a, b) => a ++ Set(b)) // Materialize for passing

    import sparkSession.implicits._
    implicit def single[A](implicit c: ClassTag[A]): Encoder[A] = Encoders.kryo[A](c)
    implicit def tuple2[A1, A2](
      implicit e1: Encoder[A1],
      e2 : Encoder[A2]
    ): Encoder[(A1, A2)] = Encoders.tuple[A1, A2](e1, e2)

    val parquetFilePattern = """.*\.(parquet|parquet\.[0-9a-zA-z]+)$"""
    val parquetFiles = dataFiles.filter(_.matches(parquetFilePattern))
    val parquetParsedRowsOpt = if (parquetFiles.nonEmpty) Some({
      val parquetSources = sparkSession.read.load(parquetFiles: _*)
      parquetSources.mapPartitions(ParseAndGranularizeRow(dataSchema, passableIntervals))
    }) else None

    val textFiles = dataFiles.filterNot(_.matches(parquetFilePattern))
    val textParsedRowsOpt = if (textFiles.nonEmpty) Some({
      val textSources = sparkSession.read.text(textFiles: _*)
      textSources.mapPartitions(ParseAndGranularizeRow(dataSchema, passableIntervals))
    }) else None

    val baseData = (parquetParsedRowsOpt, textParsedRowsOpt) match {
      case (Some(ps), Some(ts)) => ps.union(ts).distinct()
      case (Some(ps), None) => ps
      case (None, Some(ts)) => ts
      case (None, None) => throw new Exception("No files specified to parse!")
    }

    logInfo("Starting uniques")
    val optionalDims: Option[Set[String]] =
      if (dataSchema.getDelegate.getParser.getParseSpec.getDimensionsSpec.hasCustomDimensions) {
        val parseSpec = dataSchema.getDelegate.getParser.getParseSpec
        Some(parseSpec.getDimensionsSpec.getDimensionNames.asScala.toSet)
      } else {
        None
      }

    val uniquesEvents = optionalDims match {
      case Some(dims) => baseData.map { case (k, v) => (k, v.asScala.filterKeys(dims.contains).asJava) }
      case None => baseData
    }

    val partitionMap: Map[Long, Long] = uniquesEvents.rdd.countApproxDistinctByKey(
      0.05,
      new DateBucketPartitioner(dataSchema.getDelegate.getGranularitySpec.getSegmentGranularity, passableIntervals)
    ).map { x =>
      dataSchema.getDelegate.getGranularitySpec.getSegmentGranularity.bucketStart(new DateTime(x._1)).getMillis -> x._2
    }.reduceByKey(_ + _).collect().toMap

    // Map key tuple is DateBucket, PartitionInBucket with map value of Partition #
    val hashToPartitionMap: Map[(Long, Long), Int] = getSizedPartitionMap(partitionMap, rowsPerPartition)

    // Get dimension values and dims per timestamp
    hashToPartitionMap.foreach { a: ((Long, Long), Int) =>
      logInfo(
        s"Date Bucket [${a._1._1}] with partition number [${a._1._2}]" +
          s" has total partition number [${a._2}]"
      )
    }

    val indexSpec_passable = new SerializedJson[IndexSpec](indexSpec)

    val partitioned_data = baseData.rdd.map(_ -> 0)
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
          val pusher: DataSegmentPusher = SerializedJsonStatic.injector
            .getInstance(classOf[DataSegmentPusher])
          val tmpPersistDir = Files.createTempDirectory("persist").toFile
          val tmpMergeDir = Files.createTempDirectory("merge").toFile
          val closer = Closer.create()
          closer.register(new Closeable() {
            override def close(): Unit = {
              FileUtils.deleteDirectory(tmpPersistDir)
              FileUtils.deleteDirectory(tmpMergeDir)
            }
          })
          try {
            // Fail early if hadoop config is screwed up
            val hadoopConf = hadoopConfig.getDelegate
            val outPath = new Path(outPathString)
            val dimSpec = dataSchema.getDelegate.getParser.getParseSpec.getDimensionsSpec
            val excludedDims = dimSpec.getDimensionExclusions.asScala
            val finalDims: Option[util.List[String]] =
              if (dimSpec.hasCustomDimensions)
                Some(dimSpec.getDimensionNames)
              else
                None
            val finalStaticIndexer = StaticIndex.INDEX_MERGER_V9

            val groupedRows = it.grouped(rowsPerPersist)
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
              rs.foreach { r =>
                incrementalIndex.add(
                  incrementalIndex.formatRow(
                    new MapBasedInputRow(
                      r._1._1,
                      finalDims.getOrElse((r._1._2.keySet().asScala -- excludedDims).toList.asJava),
                      r._1._2
                    )
                  )
                )
              }
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
              indicesList.map(_.asInstanceOf[IndexableAdapter]).toBuffer.asJava,
              true,
              aggs.map(_.getDelegate),
              tmpMergeDir,
              indexSpec_passable.getDelegate
            )
            val allDimensions: util.List[String] = indicesList
              .map(_.getDimensionNames.asScala)
              .foldLeft(Set[String]())(_ ++ _)
              .toList.asJava
            val dataSegmentTemplate = new DataSegment(
              dataSource,
              timeInterval,
              dataSegmentVersion,
              null,
              allDimensions,
              aggs.map(_.getDelegate.getName).toList.asJava,
              if (partitionCount == 1) {
                new NumberedShardSpec(0, 1)
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

  def logDebug(str: String): Unit = {
    log.debug(str, null)
  }

  def logError(str: String, t: Throwable): Unit = {
    log.error(t, str, null)
  }

  def logTrace(str: String): Unit = {
    log.trace(str, null)
  }

  private def registerSparkListeners(
      sparkSession: SparkSession,
      dataSource  : String,
      ingestIntervals: Iterable[Interval]
  ): Unit = {
    val lifecycle = SerializedJsonStatic.lifecycle
    val emitter = SerializedJsonStatic.emitter
    sparkSession.sparkContext.addSparkListener(new SparkListener() {
      // Emit metrics at the end of each stage
      override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
        val dimensions = Map(
          "appId" -> sparkSession.sparkContext.applicationId,
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
          logDebug("Emitting metric: %s".format(aggName))
          val eventBuilder = ServiceMetricEvent.builder()
          dimensions foreach { case (n, v) => eventBuilder.setDimension(n, v) }
          emitter.emit(
            eventBuilder.build(aggName, value)
          )
        }
      }

      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        lifecycle.stop()
      }
    })
  }
}
