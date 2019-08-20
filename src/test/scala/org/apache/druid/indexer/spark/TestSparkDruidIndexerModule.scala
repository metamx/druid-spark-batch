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

import java.util.ServiceLoader

import com.google.inject.name.Names
import com.google.inject.{Binder, Key, Module}
import org.apache.druid.guice.annotations.Self
import org.apache.druid.guice.{GuiceInjectors, JsonConfigProvider}
import org.apache.druid.initialization.{DruidModule, Initialization}
import org.apache.druid.server.DruidNode
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class TestSparkDruidIndexerModule extends FlatSpec with Matchers {
  "SparkDruidIndexerModules" should "load properly" in {
    val loader: ServiceLoader[DruidModule] = ServiceLoader.load(
      classOf[DruidModule],
      classOf[TestSparkDruidIndexerModule].getClassLoader
    )
    val module = loader.asScala.head
    module.getClass.getCanonicalName should startWith("org.apache.druid.indexer.spark.SparkDruidIndexerModule")
    Initialization.makeInjectorWithModules(
      GuiceInjectors.makeStartupInjector(), Seq(
        new Module() {
          override def configure(binder: Binder): Unit = {
            JsonConfigProvider
              .bindInstance(
                binder,
                Key.get(classOf[DruidNode], classOf[Self]),
                new DruidNode(
                  "spark-indexer-test",
                  null,
                  false,
                  null,
                  null,
                  true,
                  false)
              )
            binder.bindConstant.annotatedWith(Names.named("servicePort")).to(0)
            binder.bindConstant.annotatedWith(Names.named("tlsServicePort")).to(-1)
          }
        },
        module
      ).asJava
    )
  }
}
