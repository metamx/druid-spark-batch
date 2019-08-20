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

import java.nio.ByteBuffer
import java.util.{Map => JMap}

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.druid.data.input.impl.{InputRowParser, StringInputRowParser}
import org.apache.druid.data.input.parquet.avro.ParquetAvroHadoopInputRowParser
import org.apache.druid.data.input.{InputRow, MapBasedInputRow}
import org.apache.druid.java.util.common.logger.Logger
import org.apache.druid.segment.indexing.DataSchema
import org.apache.druid.segment.transform.TransformingInputRowParser
import org.apache.spark.sql.Row
import org.apache.spark.sql.avro.SchemaConverters.toAvroType
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.joda.time.{DateTime, Interval}

import scala.collection.JavaConverters._

/**
  * Parser and data granularizer which transforms SparkSQL Row to Druid InputRow based on
  * ingestion buckets
  */
case class ParseAndGranularizeRow(
  dataSchema: SerializedJson[DataSchema],
  ingestionIntervals: Set[Interval]
) extends (Iterator[Row] => Iterator[(Long, JMap[String, Object])]) {
  private lazy val log = new Logger(getClass)

  def apply(sqlRows: Iterator[Row]): Iterator[(Long, JMap[String, Object])] = {
    val parsedRows = sqlRows
      .flatMap(parseInputRow(_, dataSchema))
      .filter(row => ingestionIntervals.exists(_.contains(row.getTimestamp)))
    parsedRows.map { row =>
      val queryGran = dataSchema.getDelegate.getGranularitySpec.getQueryGranularity
      val segmentGran = dataSchema.getDelegate.getGranularitySpec.getSegmentGranularity
      val queryGranStartTime = queryGran.bucketStart(new DateTime(row.getTimestampFromEpoch)).getMillis
      val segmentGranStartTime = segmentGran.bucketStart(new DateTime(row.getTimestampFromEpoch)).getMillis
      val bucketTime = if (queryGranStartTime >= 0) queryGranStartTime else segmentGranStartTime
      bucketTime -> row.asInstanceOf[MapBasedInputRow].getEvent
    }
  }

  private def parseInputRow(inputRow: Row, dataSchema: SerializedJson[DataSchema]): Seq[InputRow] = {
    dataSchema.getDelegate.getParser match {
      case x: StringInputRowParser =>
        x.parseBatch(ByteBuffer.wrap(inputRow.mkString.getBytes)).asScala
      case x: ParquetAvroHadoopInputRowParser =>
        x.parseBatch(convertSparkRowToAvro(inputRow)).asScala
      case x: TransformingInputRowParser[_] =>
        x.asInstanceOf[TransformingInputRowParser[GenericRecord]].parseBatch(convertSparkRowToAvro(inputRow)).asScala
      case x =>
        log.trace(
          "Could not figure out how to handle class " +
            s"[${x.getClass.getCanonicalName}]. " +
            "Hoping it can handle string input"
        )
        x.asInstanceOf[InputRowParser[Any]].parseBatch(inputRow).asScala
    }
  }

  private def convertSparkRowToAvro(inputRow: Row): GenericRecord = {
    val rowWithSchema = inputRow.asInstanceOf[GenericRowWithSchema]
    val avroRecord: GenericRecord = new GenericData.Record(toAvroType(rowWithSchema.schema))
    rowWithSchema.schema.fields.foreach(field => avroRecord.put(field.name, rowWithSchema.getAs[AnyRef](field.name)))
    avroRecord
  }
}
