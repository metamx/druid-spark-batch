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

import java.util.Collections

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.inject.name.Names
import com.google.inject.{Binder, Injector, Key, Module}
import org.apache.druid.guice.annotations.{Json, Self}
import org.apache.druid.guice.{GuiceInjectors, JsonConfigProvider}
import org.apache.druid.initialization.Initialization
import org.apache.druid.java.util.common.lifecycle.Lifecycle
import org.apache.druid.java.util.common.logger.Logger
import org.apache.druid.java.util.emitter.service.ServiceEmitter
import org.apache.druid.server.DruidNode

import scala.util.control.NonFatal

object SerializedJsonStatic {
  val LOG = new Logger("SerializedJsonStatic")
  val defaultService = "spark-indexer"
  // default indexing service port
  val defaultPort = "8090"
  val defaultTlsPort = "8290"
  lazy val injector: Injector = {
    try {
      Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), Collections.singletonList[Module](
          new Module {
            override def configure(binder: Binder): Unit = {
              binder.bindConstant().annotatedWith(Names.named("serviceName")).to(defaultService)
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(defaultPort)
              binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(defaultTlsPort)
              JsonConfigProvider.bind(binder, "druid", classOf[DruidNode], classOf[Self])
            }
          }
        )
      )
    }
    catch {
      case NonFatal(e) =>
        LOG.error(e, "Error initializing injector")
        throw e
    }
  }

  lazy val mapper: ObjectMapper = {
    try {
      injector
        .getInstance(Key.get(classOf[ObjectMapper], classOf[Json]))
        .copy()
        .disable(JsonParser.Feature.AUTO_CLOSE_SOURCE)
        .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
    }
    catch {
      case NonFatal(e) =>
        LOG.error(e, "Error getting object mapper instance")
        throw e
    }
  }

  lazy val lifecycle: Lifecycle = {
    try {
      injector.getInstance(classOf[Lifecycle])
    } catch {
      case NonFatal(e) =>
        LOG.error(e, "Error getting life cycle instance")
        throw e
    }
  }

  lazy val emitter: ServiceEmitter = {
    try {
      injector.getInstance(classOf[ServiceEmitter])
    } catch {
      case NonFatal(e) =>
        LOG.error(e, "Error getting service emitter instance")
        throw e
    }
  }

  def getLog = LOG

  val mapTypeReference = new TypeReference[java.util.Map[String, Object]] {}
}
