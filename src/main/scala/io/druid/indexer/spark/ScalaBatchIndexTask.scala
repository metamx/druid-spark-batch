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

import java.util.{Objects, Properties}

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.google.common.base.{Preconditions, Strings}
import com.metamx.common.logger.Logger
import io.druid.data.input.impl.ParseSpec
import io.druid.indexing.common.task.{AbstractFixedIntervalTask, AbstractTask}
import io.druid.indexing.common.{TaskStatus, TaskToolbox}
import io.druid.query.aggregation.AggregatorFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.Interval

import scala.collection.JavaConversions._

@JsonCreator
class ScalaBatchIndexTask(
  @JsonProperty("id")
  id: String,
  @JsonProperty("dataSource")
  dataSource: String,
  @JsonProperty("interval")
  interval: Interval,
  @JsonProperty("dataFile")
  dataFile: String,
  @JsonProperty("parseSpec")
  parseSpec: ParseSpec,
  @JsonProperty("outputPath")
  outPathString: String,
  @JsonProperty("metrics")
  aggregatorFactories: java.util.List[AggregatorFactory] = List(),
  @JsonProperty("rowsPerPartition")
  rowsPerPartition: Long = 5000000,
  @JsonProperty("rowsFlushBoundary")
  rowsPerPersist: Int = 80000,
  @JsonProperty("properties")
  properties: Properties =
  Seq(
    ("user.timezone", "UTC"),
    ("file.encoding", "UTF-8"),
    ("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager"),
    ("org.jboss.logging.provider", "log4j2"),
    ("druid.processing.columnCache.sizeBytes", "1000000000")
  ).foldLeft(new Properties())(
      (p, e) => {
        p.setProperty(e._1, e._2)
        p
      }
    ),
  @JsonProperty("master")
  master: String = "local[1]"
  ) extends AbstractFixedIntervalTask(
  if (id == null) {
    AbstractTask
      .makeId(null, ScalaBatchIndexTask.TASK_TYPE, dataSource, interval)
  }
  else {
    id
  }, dataSource, interval
)
{
  override def getType: String = ScalaBatchIndexTask.TASK_TYPE

  override def run(toolbox: TaskToolbox): TaskStatus = {
    Preconditions.checkNotNull(Strings.emptyToNull(dataFile), "%s", "dataFile")
    Preconditions.checkNotNull(Strings.emptyToNull(dataSource), "%s", "dataSource")
    Preconditions.checkNotNull(parseSpec, "%s", "parseSpec")
    Preconditions.checkNotNull(interval, "%s", "interval")
    Preconditions.checkNotNull(Strings.emptyToNull(outPathString), "%s", "outputPath")

    var conf = asScalaSet(properties.entrySet()).foldLeft(
      new SparkConf()
        .setAppName(getId)
        .setMaster(master)
    )((m, e) => m.set(e.getKey.toString, e.getValue.toString))
    val sc = new SparkContext(conf)
    var status = TaskStatus.failure(getId)
    try {
      val dataSegments = SparkDruidIndexer.loadData(
        dataFile,
        dataSource,
        new SerializedJson[ParseSpec](parseSpec),
        interval,
        aggregatorFactories,
        rowsPerPartition,
        rowsPerPersist,
        outPathString,
        sc
      )
      toolbox.pushSegments(dataSegments)
      status = TaskStatus.success(getId)
    }
    catch {
      case t: Throwable => ScalaBatchIndexTask.log.error(t, "Error in task [%s]", getId)
    }
    finally {
      sc.stop()
    }
    status
  }

  override def equals(o: Any): Boolean = {
    if (!super.equals(o)) {
      return false
    }
    if (o == null || this == null) {
      return false
    }
    if (!o.isInstanceOf[ScalaBatchIndexTask]) {
      return false
    }
    val other = o.asInstanceOf[ScalaBatchIndexTask]
    this.getAggregatorFactories.equals(other.getAggregatorFactories) &&
      Objects.equals(getDataFile, other.getDataFile) &&
      Objects.equals(getFormattedDataSource, other.getFormattedDataSource) &&
      Objects.equals(getFormattedId, other.getFormattedId) &&
      Objects.equals(getMaster, other.getMaster) &&
      Objects.equals(getOutputPath, other.getOutputPath) &&
      Objects.equals(getProperties, other.getProperties) &&
      Objects.equals(getRowsPerPartition, other.getRowsPerPartition) &&
      Objects.equals(getRowsPerPersist, other.getRowsPerPersist) &&
      Objects.equals(getTotalInterval, other.getTotalInterval) &&
      Objects.equals(
        getParseSpec
          .getDimensionsSpec
          .getDimensionExclusions,
        other.getParseSpec.getDimensionsSpec.getDimensionExclusions
      ) &&
      Objects.equals(
        getParseSpec.getDimensionsSpec.getDimensions,
        other.getParseSpec.getDimensionsSpec.getDimensions
      ) &&
      Objects.equals(
        getParseSpec
          .getDimensionsSpec
          .getSpatialDimensions,
        other.getParseSpec.getDimensionsSpec.getSpatialDimensions
      ) &&
      Objects.equals(
        parseSpec.getTimestampSpec.getMissingValue,
        other.getParseSpec.getTimestampSpec.getMissingValue
      ) &&
      Objects.equals(
        parseSpec
          .getTimestampSpec
          .getTimestampColumn,
        other.getParseSpec.getTimestampSpec.getTimestampColumn
      ) &&
      Objects.equals(
        getParseSpec
          .getTimestampSpec
          .getTimestampFormat,
        other.getParseSpec.getTimestampSpec.getTimestampFormat
      )
  }

  @JsonProperty("id")
  def getFormattedId = getId

  @JsonProperty("dataSource")
  def getFormattedDataSource = getDataSource

  @JsonProperty("interval")
  def getTotalInterval = interval

  @JsonProperty("dataFile")
  def getDataFile = dataFile

  @JsonProperty("parseSpec")
  def getParseSpec = parseSpec

  @JsonProperty("metrics")
  def getAggregatorFactories = aggregatorFactories

  @JsonProperty("outputPath")
  def getOutputPath = outPathString

  @JsonProperty("rowsPerPartition")
  def getRowsPerPartition = rowsPerPartition

  @JsonProperty("rowsFlushBoundary")
  def getRowsPerPersist = rowsPerPersist

  @JsonProperty("properties")
  def getProperties = properties

  @JsonProperty("master")
  def getMaster = master
}

object ScalaBatchIndexTask
{
  val log       = new Logger(ScalaBatchIndexTask.getClass)
  val TASK_TYPE = "index_scala"
}
