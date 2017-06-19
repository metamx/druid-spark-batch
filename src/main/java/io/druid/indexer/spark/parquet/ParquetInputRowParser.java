package io.druid.indexer.spark.parquet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.ParseSpec;
import org.apache.parquet.tools.read.SimpleParquetRecord;
import org.apache.parquet.tools.read.SimpleRecord;

public class ParquetInputRowParser implements InputRowParser<SimpleRecord> {
  private final ParseSpec parseSpec;
  private final MapInputRowParser mapParser;

  @JsonCreator
  public ParquetInputRowParser(
          @JsonProperty("parseSpec") ParseSpec parseSpec
  )
  {
    this.parseSpec = parseSpec;
    this.mapParser = new MapInputRowParser(this.parseSpec);
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
  public InputRow parse(SimpleRecord input)
  {
    // We should really create a ParquetBasedInputRow that does not need an intermediate map but accesses
    // the SimpleRecord directly...
    return mapParser.parse(SimpleParquetRecord.toJson(input));
  }
}
