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

import java.util.Properties

import org.apache.druid.indexing.common.task.Task
import org.apache.druid.java.util.common.granularity.Granularity
import org.apache.druid.segment.IndexSpec
import org.apache.druid.segment.data.{CompressionStrategy, RoaringBitmapSerdeFactory}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class TestScalaBatchIndexTask extends FlatSpec with Matchers {

  import TestConfProvider._

  "The ScalaBatchIndexTask" should "properly SerDe a full object" in {
    val taskPre = buildSparkBatchIndexTask()
    val taskPost = objectMapper.readValue(objectMapper.writeValueAsString(taskPre), classOf[SparkBatchIndexTask])
    val implVersion = classOf[Granularity].getPackage.getImplementationVersion
    taskPre.rowFlushBoundary_ should equal(taskPost.rowFlushBoundary_)
    taskPre.getDataSchema.getAggregators should equal(taskPost.getDataSchema.getAggregators)
    taskPre.getDataSchema.getDataSource should equal(taskPost.getDataSchema.getDataSource)
    taskPre.getDataSchema.getGranularitySpec should equal(taskPost.getDataSchema.getGranularitySpec)
    // Skip checking parser.. see https://github.com/druid-io/druid/issues/2914
    taskPre.getDataFiles should equal(taskPost.getDataFiles)
    taskPre.aggregatorFactories_ should equal(taskPost.aggregatorFactories_)
    taskPre.indexSpec_ should equal(taskPost.indexSpec_)
    taskPre.master_ should equal(taskPost.master_)
    taskPre.properties_ should equal(taskPost.properties_)
    taskPre.targetPartitionSize_ should equal(taskPost.targetPartitionSize_)
    taskPre.getHadoopDependencyCoordinates should equal(taskPost.getHadoopDependencyCoordinates)
    taskPre.getHadoopDependencyCoordinates should equal(hadoopDependencyCoordinates)
    taskPre.getBuildV9Directly should equal(taskPost.getBuildV9Directly)
  }

  it should "properly deserialize" in {
    val taskPre: SparkBatchIndexTask = buildSparkBatchIndexTask()
    val task: Task = objectMapper.readValue(TestConfProvider.taskConfURL, classOf[Task])
    task.getContext shouldBe 'Empty
    assertResult(SparkBatchIndexTask.TASK_TYPE_BASE)(task.getType)

    /** https://github.com/druid-io/druid/issues/2914
      * taskPre should ===(task)
      */
    task.asInstanceOf[SparkBatchIndexTask].getDataSchema.getParser.getParseSpec should
      ===(taskPre.getDataSchema.getParser.getParseSpec)
    task.asInstanceOf[SparkBatchIndexTask].getHadoopDependencyCoordinates.asScala should
      ===(Seq("org.apache.spark:spark-core"))
    task.asInstanceOf[SparkBatchIndexTask].getBuildV9Directly should equal(false)
  }

  it should "be equal for equal tasks" in {
    val task1 = buildSparkBatchIndexTask()
    val task2 = buildSparkBatchIndexTask()
    task1 should equal(task2)
    task2 should equal(task1)

    task1 should equal(
      buildSparkBatchIndexTask(context = Map())
    )

    task1 should equal(
      buildSparkBatchIndexTask(indexSpec = new IndexSpec())
    )
  }

  it should "not be equal for dissimilar tasks" in {
    val task1 = buildSparkBatchIndexTask()
    task1 should not equal buildSparkBatchIndexTask(id = taskId + "something else")

    /** DataSchema compare is busted
      * task1 should not equal
      * buildSparkBatchIndexTask(dataSchema = buildDataSchema(dataSource = dataSource + "something else"))
      */

    task1 should not equal buildSparkBatchIndexTask(dataFiles = dataFiles ++ List("something else"))

    task1 should not equal buildSparkBatchIndexTask(rowsPerPartition = rowsPerPartition + 1)


    task1 should not equal buildSparkBatchIndexTask(rowsPerPersist = rowsPerFlush + 1)

    task1 should not equal buildSparkBatchIndexTask(master = master + "something else")

    val notSameProperties = new Properties(properties)
    notSameProperties.setProperty("Something not present", "some value")
    task1 should not equal buildSparkBatchIndexTask(properties = notSameProperties)

    task1 should
      not equal
      buildSparkBatchIndexTask(
        indexSpec = new IndexSpec(
          new RoaringBitmapSerdeFactory(false),
          CompressionStrategy.LZ4,
          CompressionStrategy.LZ4,
          null
        )
      )

    task1 should not equal buildSparkBatchIndexTask(context = Map[String, Object]("test" -> "oops"))

    task1 should not equal buildSparkBatchIndexTask(classpathPrefix = "someOther.jar")

    task1 should not equal buildSparkBatchIndexTask(buildV9Directly = false)
  }

  it should "default buildV9Directly to false if not specified" in {

    val taskWithoutV9 = buildSparkBatchIndexTaskWithoutV9()
    taskWithoutV9.getBuildV9Directly should equal(false)
  }
}
