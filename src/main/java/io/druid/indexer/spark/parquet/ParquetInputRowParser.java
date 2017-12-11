package io.druid.indexer.spark.parquet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.avro.AvroParsers;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;

public class ParquetInputRowParser implements InputRowParser<GenericRecord> {
  private final ParseSpec parseSpec;
  private final ObjectFlattener<GenericRecord> avroFlattener;

  @JsonCreator
  public ParquetInputRowParser(
          @JsonProperty("parseSpec") ParseSpec parseSpec
  )
  {
    this.parseSpec = parseSpec;
    this.avroFlattener = AvroParsers.makeFlattener(parseSpec, false, false);
  }

  @JsonProperty
  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public ParquetInputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new ParquetInputRowParser(parseSpec);
  }

  @Override
  public InputRow parse(GenericRecord input)
  {
    // We should really create a ParquetBasedInputRow that does not need an intermediate map but accesses
    // the SimpleRecord directly...
    MapBasedInputRow mapBasedInputRow = (MapBasedInputRow) AvroParsers.parseGenericRecord(input, parseSpec, avroFlattener);
    Map<String, Object> event = mapBasedInputRow.getEvent();
    Map<String, Object> newMap = Maps.newHashMapWithExpectedSize(event.size());
    for (String key : mapBasedInputRow.getEvent().keySet()) {
      newMap.put(key, event.get(key));
    }
    return new MapBasedInputRow(mapBasedInputRow.getTimestampFromEpoch(), mapBasedInputRow.getDimensions(), newMap);
  }
}
