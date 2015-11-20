package io.druid.indexer.spark2

import java.nio.file.Files

import com.google.common.io.Closer
import io.druid.data.input.MapBasedInputRow
import io.druid.data.input.impl.InputRowParser
import io.druid.indexer.JobHelper
import io.druid.query.aggregation.AggregatorFactory
import io.druid.segment.{IndexMerger, IndexIO, QueryableIndexIndexableAdapter, IndexSpec}
import io.druid.segment.incremental.{IncrementalIndexSchema, OnheapIncrementalIndex}
import io.druid.segment.indexing.DataSchema
import io.druid.segment.loading.DataSegmentPusher
import io.druid.timeline.DataSegment
import io.druid.timeline.partition.{NumberedShardSpec, NoneShardSpec}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.util.Progressable
import org.apache.spark.Logging
import org.joda.time.Interval
import io.druid.indexer.spark2.Closeable._
import scala.collection.JavaConversions._

case class DruidPartitionIndexer(val _dataSchema: SerializedJson[DataSchema],
                                 val _ingestInterval: Interval,
                                 val rowsPerPersist: Int,
                                 val druidPartitionMap: DruidPartitionIndexMap,
                                 val _indexSpec: SerializedJson[IndexSpec],
                                 val hadoopConfig: SerializedHadoopConfig,
                                 val outPathString: String,
                                 val dataSegmentVersion: String)
  extends Function2[Int, Iterator[DruidIndexableRow], Iterator[SerializedJson[DataSegment]]] with Logging {

  @transient
  lazy implicit val dataSchema: DataSchema = _dataSchema

  @transient
  lazy implicit val ingestInterval: Interval = _ingestInterval

  @transient
  lazy val aggs: Array[AggregatorFactory] = dataSchema.getAggregators

  @transient
  lazy val dimensions = dataSchema.getParser.getParseSpec.getDimensionsSpec.getDimensions

  @transient
  lazy val indexSpec: IndexSpec = _indexSpec

  @transient
  lazy implicit val closer = Closer.create()

  @transient
  lazy val tmpPersistDir = Files.createTempDirectory("persist").toFile registerForClose

  @transient
  lazy val tmpMergeDir = Files.createTempDirectory("merge").toFile registerForClose

  @transient
  lazy val hadoopConf = hadoopConfig.getDelegate

  @transient
  lazy val dataSource = dataSchema.getDelegate.getDataSource

  def apply(index: Int, it: Iterator[DruidIndexableRow]):
  Iterator[SerializedJson[DataSegment]] = {

    val partitionInfo: DruidPartitionInfo = druidPartitionMap(index)
    log.info("Creating index [%s] for date range [%s]"
      format(partitionInfo.partitionNum, partitionInfo.interval))

    val parser: InputRowParser[_] = SerializedJsonStatic.mapper
      .convertValue(dataSchema.getParserMap, classOf[InputRowParser[_]])

    val rows = it

    val pusher: DataSegmentPusher = SerializedJsonStatic.injector.getInstance(classOf[DataSegmentPusher])


    def makeQuerybaleIndex(incIndex: OnheapIncrementalIndex): QueryableIndexIndexableAdapter = {
      new QueryableIndexIndexableAdapter(IndexIO.loadIndex(
        IndexMerger.persist(
          incIndex, partitionInfo.interval, tmpPersistDir, null,
          indexSpec) registerForClose
      )
      )
    }

    def makeOnHeapIncrmentalIndex(input: Iterable[DruidIndexableRow]): OnheapIncrementalIndex = {

      val idx = new OnheapIncrementalIndex(
        new IncrementalIndexSchema.Builder()
          .withDimensionsSpec(parser)
          .withQueryGranularity(dataSchema.getGranularitySpec.getQueryGranularity)
          .withMetrics(aggs)
          .withMinTimestamp(partitionInfo.timeBucket)
          .build()
        , rowsPerPersist
      ) registerForClose

      input.foreach(r => idx.add(new MapBasedInputRow(r._1._1, dimensions, r._1._2)))

      idx
    }

    try {
      // Fail early if hadoop config is screwed up
      val hadoopConf = hadoopConfig.getDelegate
      val outPath = new Path(outPathString)
      val hadoopFs = outPath.getFileSystem(hadoopConf)

      val file = IndexMerger.merge(
        rows.grouped(rowsPerPersist).map(makeOnHeapIncrmentalIndex).map(makeQuerybaleIndex).toSeq,
        aggs.map(_.getDelegate),
        tmpMergeDir,
        null,
        indexSpec,
        PI
      )
      val dataSegment = JobHelper.serializeOutIndex(
        new DataSegment(
          dataSource,
          partitionInfo.interval,
          dataSegmentVersion,
          null,
          dimensions,
          aggs.map(_.getDelegate.getName).toList,
          if (partitionInfo.count == 1) {
            new NoneShardSpec()
          } else {
            new NumberedShardSpec(partitionInfo.partitionNum, partitionInfo.count)
          },
          -1,
          -1
        ),
        hadoopConf,
        new Progressable {
          override def progress(): Unit = log.debug("Progress")
        },
        new TaskAttemptID(new org.apache.hadoop.mapred.TaskID(), index),
        file,
        JobHelper.makeSegmentOutputPath(
          outPath,
          hadoopFs,
          dataSource,
          dataSegmentVersion,
          partitionInfo.interval,
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
}

