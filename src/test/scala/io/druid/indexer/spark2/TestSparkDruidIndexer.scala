package io.druid.indexer.spark2

import java.io.File
import java.nio.file.Files

import com.google.common.io.Closer
import com.metamx.common.CompressionUtils
import io.druid.common.utils.JodaUtils
import io.druid.indexer.spark.TestScalaBatchIndexTask
import io.druid.segment.{IndexIO, QueryableIndexIndexableAdapter}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class TestSparkDruidIndexer extends FlatSpec with Matchers
{

  import Closeable._
  import TestScalaBatchIndexTask._

  "The spark indexer" should "return proper DataSegments" in {
    val data_file = this.getClass.getResource("/lineitem.small.tbl").toString
    implicit  val closer = Closer.create()
    val outDir = Files.createTempDirectory("segments").toFile
    (outDir.mkdirs() || outDir.exists()) && outDir.isDirectory should be(true)
    outDir.registerForClose

    try {
      implicit val sqlContext = TestSQLContext
      sqlContext registerForClose

      val loadResults = SparkDruidIndexer.loadData(
        lineItemDF(data_file),
        new SerializedJson(dataSchema),
        interval,
        rowsPerPartition,
        rowsPerFlush,
        outDir.toString,
        indexSpec,
        sqlContext
      )
      loadResults.length should be(7)
      for (
        segment <- loadResults
      ) {
        segment.getBinaryVersion should be(9)
        segment.getDataSource should equal(dataSource)
        interval.contains(segment.getInterval) should be(true)
        segment.getInterval.contains(interval) should be(false)
        segment.getSize should be > 0L
        segment.getDimensions should equal(dataSchema.getParser.getParseSpec.getDimensionsSpec.getDimensions)
        segment.getMetrics.asScala.toList should equal(dataSchema.getAggregators.map(_.getName).toList)
        val file = new File(segment.getLoadSpec.get("path").toString)
        file.exists() should be(true)
        val segDir = Files.createTempDirectory(outDir.toPath, "loadableSegment-%s" format segment.getIdentifier).toFile
        val copyResult = CompressionUtils.unzip(file, segDir)
        copyResult.size should be > 0L
        copyResult.getFiles.asScala.map(_.getName).toSet should equal(Set("00000.smoosh", "meta.smoosh", "version.bin"))
        val index = IndexIO.loadIndex(segDir)
        try {
          val qindex = new QueryableIndexIndexableAdapter(index)
          qindex.getDimensionNames.asScala.toList should
            equal(dataSchema.getParser.getParseSpec.getDimensionsSpec.getDimensions.asScala.toList)
          for (dimension <- qindex.getDimensionNames.iterator().asScala) {
            val dimVal = qindex.getDimValueLookup(dimension).asScala
            dimVal should not be 'Empty
            for (dv <- dimVal) {
              dv should not startWith "List("
              dv should not startWith "Set("
            }
          }
          qindex.getNumRows should be > 0
          for(colName <- Seq("count")) {
            val column = index.getColumn(colName).getGenericColumn
            try {
              for (i <- Range.apply(0, qindex.getNumRows)) {
                column.getLongSingleValueRow(i) should not be 0
              }
            } finally {
              column.close()
            }
          }
          for(colName <- Seq("L_QUANTITY_longSum")) {
            val column = index.getColumn(colName).getGenericColumn
            try {
              Range.apply(0, qindex.getNumRows).map(column.getLongSingleValueRow).sum should not be 0
            } finally {
              column.close()
            }
          }
          for(colName <- Seq("L_DISCOUNT_doubleSum", "L_TAX_doubleSum")) {
            val column = index.getColumn(colName).getGenericColumn
            try {
              Range.apply(0, qindex.getNumRows).map(column.getFloatSingleValueRow).sum should not be 0.0D
            } finally {
              column.close()
            }
          }
          index.getDataInterval.getEnd.getMillis should not be (JodaUtils.MAX_INSTANT)
        }
        finally {
          index.close()
        }
      }
    }
    finally {
      closer.close()
    }
  }
}
