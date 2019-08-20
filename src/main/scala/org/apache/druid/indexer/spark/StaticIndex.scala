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

import org.apache.druid.segment.column.ColumnConfig
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory
import org.apache.druid.segment.{IndexIO, IndexMergerV9}

object StaticIndex {
  val INDEX_IO = new IndexIO(
    SerializedJsonStatic.mapper,
    new ColumnConfig {
      override def columnCacheSizeBytes(): Int = 1000000
    }
  )

  val INDEX_MERGER_V9 = new IndexMergerV9(
    SerializedJsonStatic.mapper,
    INDEX_IO,
    TmpFileSegmentWriteOutMediumFactory.instance()
  )
}
