package io.druid.indexer.spark.parquet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.InputRow;
import io.druid.data.input.avro.AvroParsers;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.avro.generic.GenericRecord;

import java.util.List;

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
  public List<InputRow> parseBatch(GenericRecord input)
  {
    // We should really create a ParquetBasedInputRow that does not need an intermediate map but accesses
    // the SimpleRecord directly...
    return AvroParsers.parseGenericRecord(input, parseSpec, avroFlattener);
  }
}
