package org.apache.parquet.tools.read;

import com.google.common.collect.Maps;

import java.util.Map;

public class SimpleParquetRecord extends SimpleRecord {
  public static Map<String, Object> toJson(SimpleRecord record) {
    Map<String, Object> result = Maps.newLinkedHashMap();
    for (NameValue value : record.getValues()) {
      result.put(value.getName(), toJsonValue(value.getValue()));
    }
    return result;
  }
}
