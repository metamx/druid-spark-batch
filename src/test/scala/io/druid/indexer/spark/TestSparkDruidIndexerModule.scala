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

import com.google.inject.Binder
import com.google.inject.Key
import com.google.inject.Module
import io.druid.guice.GuiceInjectors
import io.druid.guice.JsonConfigProvider
import io.druid.guice.annotations.Self
import io.druid.initialization.DruidModule
import io.druid.initialization.Initialization
import io.druid.server.DruidNode
import io.druid.server.initialization.ServerConfig
import java.util.ServiceLoader
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import scala.collection.JavaConverters._

class TestSparkDruidIndexerModule extends FlatSpec with Matchers
{
  "SparkDruidIndexerModules" should "load properly" in {
    val loader: ServiceLoader[DruidModule] = ServiceLoader.load(classOf[DruidModule], classOf[TestSparkDruidIndexerModule].getClassLoader)
    val module: DruidModule = loader.asScala.head
    module.getClass.getCanonicalName should startWith("io.druid.indexer.spark.SparkDruidIndexerModule")
    Initialization.makeInjectorWithModules(
      GuiceInjectors.makeStartupInjector(), Seq(
        new Module()
        {
          override def configure(binder: Binder) = {
            JsonConfigProvider
              .bindInstance(
                binder,
                Key.get(classOf[DruidNode], classOf[Self]),
                new DruidNode("spark-indexer-test", null, null, null, new ServerConfig)
              )
          }
        },
        module
      ).asJava
    )
  }
}
