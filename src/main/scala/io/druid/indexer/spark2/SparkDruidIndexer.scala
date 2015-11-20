package io.druid.indexer.spark2

import java.util.{Arrays => jArrays}
import com.metamx.common.logger.Logger
import io.druid.data.input.MapBasedInputRow
import io.druid.data.input.impl.MapInputRowParser
import io.druid.query.aggregation.AggregatorFactory
import io.druid.segment._
import io.druid.segment.indexing.DataSchema
import io.druid.timeline.DataSegment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, Interval}

import scala.collection.JavaConversions._

case class GranularizeData(dataSchema: SerializedJson[DataSchema],
                           ingestInterval: Interval,
                           schema: StructType)
  extends Function1[Iterator[Row], Iterator[GranularizedDruidRow]] {

  def apply(it: Iterator[Row]): Iterator[GranularizedDruidRow] = {
    val mapInputRowParser: SerializedJson[MapInputRowParser] =
      new MapInputRowParser(dataSchema.getParser.getParseSpec)

    implicit val dtFmt: DateTimeFormatter = DateTimeFormat.forPattern(
      dataSchema.getParser.getParseSpec.getTimestampSpec.getTimestampFormat
    )

    val i = it.map(r => mapInputRowParser.parse(mapRow(schema, r)))
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
  }
}


object SparkDruidIndexer {

  implicit val log = new Logger(SparkDruidIndexer.getClass)

  def loadData(
                sourceDF: DataFrame,
                dataSchema: DataSchema,
                ingestInterval: Interval,
                rowsPerPartition: Long,
                rowsPerPersist: Int,
                outPathString: String,
                indexSpec: IndexSpec,
                sqlContext: SQLContext
              ): Seq[DataSegment] = {
    loadData(sourceDF.queryExecution.logical,
      dataSchema,
      ingestInterval,
      rowsPerPartition,
      rowsPerPersist,
      outPathString,
      indexSpec,
      sqlContext)

  }

  def loadData(
                source: LogicalPlan,
                dataSchema: DataSchema,
                ingestInterval: Interval,
                rowsPerPartition: Long,
                rowsPerPersist: Int,
                outPathString: String,
                indexSpec: IndexSpec,
                sqlContext: SQLContext
              ): Seq[DataSegment] = {

    implicit val _dS: SerializedJson[DataSchema] = dataSchema
    implicit val _inI = ingestInterval

    val dataSource = dataSchema.getDelegate.getDataSource
    log.info("Launching Spark task with jar version [%s]", getClass.getPackage.getImplementationVersion)
    val dataSegmentVersion = DateTime.now().toString
    val hadoopConfig = new SerializedHadoopConfig(sqlContext.sparkContext.hadoopConfiguration)
    log.info("Starting caching of raw data for [%s] over interval [%s]", dataSource, ingestInterval)

    val dF = new DataFrame(sqlContext, source)
    val schema = dF.schema

    /*
     * Step 1: add Query Granularity to each row.
     */
    val baseData: RDD[GranularizedDruidRow] =
      dF.mapPartitions(GranularizeData(dataSchema, ingestInterval, schema))

    log.info("Starting uniqes")

    /*
     * Step 2: build the (segment, partNum) -> index map based on rowsPerPartition
     */
    val druidPartitionIndexMap: DruidPartitionIndexMap = DruidPartitionIndexMap(baseData, rowsPerPartition)
    druidPartitionIndexMap.dumpToLog

    /*
     * Step 3: Partition data based on Segment Granularity and data hash
     */
    val partitionedStream: RDD[DruidIndexableRow] = baseData.map(_ -> 0).partitionBy(
      new DateBucketAndHashPartitioner(
        dataSchema.getDelegate.getGranularitySpec.getSegmentGranularity,
        ingestInterval,
        druidPartitionIndexMap
      )
    )

    /*
     * Step 4: Create a DataSegment for each Druid Segment
     */
    val partitioned_data = partitionedStream
      .mapPartitionsWithIndex(DruidPartitionIndexer(dataSchema,
        ingestInterval,
        rowsPerPersist,
        druidPartitionIndexMap,
        indexSpec,
        hadoopConfig,
        outPathString,
        dataSegmentVersion))

    val results = partitioned_data.cache().collect().map(_.getDelegate)
    log.info("Finished with %s", jArrays.deepToString(results.map(_.toString)))
    results.toSeq
  }

}
