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

import java.io._
import java.util

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

import scala.collection.JavaConverters._

/**
  * This is tricky. The type enforcing is only done at compile time. The JSON serde plays it fast and
  * loose with the types
  */
@SerialVersionUID(713838456349L)
class SerializedJson[A](inputDelegate: A) extends KryoSerializable with Serializable {
  @transient var delegate: A = Option(inputDelegate).getOrElse(throw new NullPointerException())

  @throws[IOException]
  private def writeObject(output: ObjectOutputStream): Unit = {
    innerWrite(output)
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    innerWrite(output)
  }

  def innerWrite(output: OutputStream): Unit = SerializedJsonStatic.mapper.writeValue(output, toJavaMap)

  def toJavaMap: util.Map[String, String] = Map(
    "class" -> delegate.getClass.getCanonicalName,
    "delegate" -> SerializedJsonStatic.mapper.writeValueAsString(delegate)
  ).asJava

  def getMap(input: InputStream): Map[String, Object] = SerializedJsonStatic
    .mapper
    .readValue(input, SerializedJsonStatic.mapTypeReference)
    .asInstanceOf[java.util.Map[String, Object]].asScala.toMap


  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readObject(input: ObjectInputStream): Unit = {
    if (SerializedJsonStatic.getLog.isTraceEnabled) {
      SerializedJsonStatic.getLog
        .trace("Reading Object")
    }
    fillFromMap(getMap(input))
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    if (SerializedJsonStatic.getLog.isTraceEnabled) {
      SerializedJsonStatic.getLog
        .trace("Reading Kryo")
    }
    fillFromMap(getMap(input))
  }

  def fillFromMap(m: Map[String, Object]): Unit = {
    val clazzName: Class[_] = m.get("class") match {
      case Some(cn) => if (Thread.currentThread().getContextClassLoader == null) {
        if (SerializedJsonStatic.getLog.isTraceEnabled) {
          SerializedJsonStatic.getLog
            .trace(s"Using class's classloader [${getClass.getClassLoader}]")
        }
        getClass.getClassLoader.loadClass(cn.toString)
      } else {
        val classLoader = Thread.currentThread().getContextClassLoader
        if (SerializedJsonStatic.getLog.isTraceEnabled) {
          SerializedJsonStatic.getLog
            .trace(s"Using context classloader [$classLoader]")
        }
        classLoader.loadClass(cn.toString)
      }
      case _ => throw new NullPointerException("Missing `class`")
    }
    delegate = m.get("delegate") match {
      case Some(d) => SerializedJsonStatic.mapper.readValue(d.toString, clazzName)
        .asInstanceOf[A]
      case _ => throw new NullPointerException("Missing `delegate`")
    }
    if (SerializedJsonStatic.getLog.isTraceEnabled) {
      SerializedJsonStatic.getLog.trace(s"Read in $delegate")
    }
  }

  def getDelegate = delegate
}
