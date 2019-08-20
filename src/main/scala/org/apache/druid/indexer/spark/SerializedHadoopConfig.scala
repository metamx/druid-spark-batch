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

import java.io.{IOException, ObjectInputStream, ObjectOutputStream, Serializable}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.apache.hadoop.conf.Configuration

@SerialVersionUID(68710585891L)
class SerializedHadoopConfig(delegate: Configuration) extends KryoSerializable with Serializable {
  @transient var del = delegate

  @throws[IOException]
  private def writeObject(out: ObjectOutputStream) = {
    if (SerializedJsonStatic.getLog.isTraceEnabled) SerializedJsonStatic.getLog
      .trace("Writing Hadoop Object")
    del.write(out)
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readObject(in: ObjectInputStream) = {
    if (SerializedJsonStatic.getLog.isTraceEnabled) SerializedJsonStatic.getLog
      .trace("Reading Hadoop Object")
    del = new Configuration()
    del.readFields(in)
  }

  def getDelegate = del

  override def write(kryo: Kryo, output: Output): Unit = {
    if (SerializedJsonStatic.getLog.isTraceEnabled) SerializedJsonStatic.getLog
      .trace("Writing Hadoop Kryo")
    writeObject(new ObjectOutputStream(output))
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    if (SerializedJsonStatic.getLog.isTraceEnabled) SerializedJsonStatic.getLog
      .trace("Reading Hadoop Kryo")
    readObject(new ObjectInputStream(input))
  }
}
