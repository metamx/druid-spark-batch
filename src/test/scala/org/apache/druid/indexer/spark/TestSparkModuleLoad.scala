package org.apache.druid.indexer.spark

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

import org.apache.druid.indexer.spark

import org.apache.druid.initialization.DruidModule
import java.util.ServiceLoader
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import scala.collection.JavaConverters._

class TestSparkModuleLoad  extends FlatSpec with Matchers
{
  "SparkDruidIndexerModules" should "load version 2.10 properly" in {
    val loader: ServiceLoader[DruidModule] = ServiceLoader.load(classOf[DruidModule], classOf[TestSparkDruidIndexerModule].getClassLoader)
    val module: DruidModule = loader.asScala.head
    module.isInstanceOf[SparkDruidIndexerModule] should be(true)
  }
}
