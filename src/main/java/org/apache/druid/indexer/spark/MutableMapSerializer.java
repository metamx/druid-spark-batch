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
package org.apache.druid.indexer.spark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.MapSerializer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A custom serializer for Kryo is needed as Druid and Kryo do not play well together. During deserialization
 * [[org.apache.druid.java.util.common.parsers.ObjectFlatteners]] returns a non modifiable Map causing an
 * java.lang.UnsupportedOperationException in [[com.esotericsoftware.kryo.serializers.MapSerializer#read]].
 * This class returns a modifiable map when MapSerializer#read calls create
 * https://github.com/apache/incubator-druid/pull/6027
 */
public class MutableMapSerializer extends MapSerializer {
  @Override
  protected Map create(Kryo kryo, Input input, Class<Map> type) {
    Map resultMap;
    if (ConcurrentMap.class.isAssignableFrom(type))
      resultMap = new ConcurrentHashMap();
    else
      resultMap = new LinkedHashMap();
    return resultMap;
  }
}
