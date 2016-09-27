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

import com.fasterxml.jackson.databind.Module
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.databind.module.SimpleModule
import com.google.inject.Binder
import io.druid.initialization.DruidModule
import java.util
import scala.collection.JavaConverters._

class SparkDruidIndexerModule210 extends DruidModule
{
  override def getJacksonModules: util.List[_ <: Module] = {
    val module = new SimpleModule("SparkDruidIndexer210")
      .registerSubtypes(
        // Only for migration purposes. Prior releases had no suffix and were only 2.10
        new NamedType(classOf[SparkBatchIndexTask], "index_spark")
      )
    List(module).asJava
  }

  override def configure(binder: Binder): Unit = {
    // NOOP
  }
}
