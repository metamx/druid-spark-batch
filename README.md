# druid-spark-batch
Druid indexing plugin for using Spark in batch jobs

This repository holds a Druid extension for using Spark as the engine for running batch jobs

To build issue the commnand `sbt clean test publish-local publish-m2`

## Default Properties
The default properties injected into spark are as follows:
```
    .set("spark.executor.memory", "7G")
    .set("spark.executor.cores", "1")
    .set("spark.kryo.referenceTracking", "false")
    .set("user.timezone", "UTC")
    .set("file.encoding", "UTF-8")
    .set("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager")
    .set("org.jboss.logging.provider", "slf4j")
```

## How to use

There are four key things that need configured to use this extension

1. Overlord needs the `druid-spark-batch` extension added.
2. MiddleManager (if present) needs the `druid-spark-batch` extension added.
3. A task json needs configured.
4. Spark is included in the default hadoop coordinates similar to `druid.indexer.task.defaultHadoopCoordinates=["org.apache.spark:spark-core_2.10:1.5.2-mmx1"]`

To load the extension, use the appropriate coordinates (for druid 0.8.x the following should be added to `druid.extensions.coordinates`  : `io.druid.extensions:druid-spark-batch_2.10:jar:assembly:0.0.13`) or make certain the extension jars are located in the proper directories (druid 0.9.x and later with version 0.9.0.x of this library)

## Task JSON
The following is an example spark batch task for the indexing service:

```json
{
    "dataFiles":["file:///Users/charlesallen/bin/wrk/lineitem.small.tbl"],
    "dataSchema": {
        "dataSource": "sparkTest",
        "granularitySpec": {
            "intervals": [
                "1992-01-01T00:00:00.000Z/1999-01-01T00:00:00.000Z"
            ],
            "queryGranularity": {
                "type": "all"
            },
            "segmentGranularity": "YEAR",
            "type": "uniform"
        },
        "metricsSpec": [
            {
                "name": "count",
                "type": "count"
            },
            {
                "fieldName": "l_quantity",
                "name": "L_QUANTITY_longSum",
                "type": "longSum"
            },
            {
                "fieldName": "l_extendedprice",
                "name": "L_EXTENDEDPRICE_doubleSum",
                "type": "doubleSum"
            },
            {
                "fieldName": "l_discount",
                "name": "L_DISCOUNT_doubleSum",
                "type": "doubleSum"
            },
            {
                "fieldName": "l_tax",
                "name": "L_TAX_doubleSum",
                "type": "doubleSum"
            }
        ],
        "parser": {
            "encoding": "UTF-8",
            "parseSpec": {
                "columns": [
                    "l_orderkey",
                    "l_partkey",
                    "l_suppkey",
                    "l_linenumber",
                    "l_quantity",
                    "l_extendedprice",
                    "l_discount",
                    "l_tax",
                    "l_returnflag",
                    "l_linestatus",
                    "l_shipdate",
                    "l_commitdate",
                    "l_receiptdate",
                    "l_shipinstruct",
                    "l_shipmode",
                    "l_comment"
                ],
                "delimiter": "|",
                "dimensionsSpec": {
                    "dimensionExclusions": [
                        "l_tax",
                        "l_quantity",
                        "count",
                        "l_extendedprice",
                        "l_shipdate",
                        "l_discount"
                    ],
                    "dimensions": [
                        "l_comment",
                        "l_commitdate",
                        "l_linenumber",
                        "l_linestatus",
                        "l_orderkey",
                        "l_receiptdate",
                        "l_returnflag",
                        "l_shipinstruct",
                        "l_shipmode",
                        "l_suppkey"
                    ],
                    "spatialDimensions": []
                },
                "format": "tsv",
                "listDelimiter": ",",
                "timestampSpec": {
                    "column": "l_shipdate",
                    "format": "yyyy-MM-dd",
                    "missingValue": null
                }
            },
            "type": "string"
        }
    },
    "dataSource": "someDataSource",
    "indexSpec": {
        "bitmap": {
            "type": "concise"
        },
        "dimensionCompression": "lz4",
        "metricCompression": "lz4"
    },
    "intervals": ["1992-01-01T00:00:00.000Z/1999-01-01T00:00:00.000Z"],
    "master": "local[1]",
    "properties": {
        "some.property": "someValue",
        "spark.io.compression.codec":"org.apache.spark.io.LZ4CompressionCodec"
    },
    "targetPartitionSize": 10000000,
    "type": "index_spark"
}
```

The json keys accepted by the spark batch indexer are described below

### Batch indexer json fields

|Field                |Type            |Required          |Default          |Description|
|---------------------|----------------|------------------|-----------------|-----------|
|`type`               |String          |Yes, `index_spark`|N/A              | Must be `index_spark`|
|`paths`              |List of strings |Yes               |N/A              |A list of hadoop-readable input files. The values are joined with a `,` and used as a `SparkContext.textFile`|
|`dataSchema`         |DataSchema      |Yes               |N/A              |The data schema to use|
|`intervals`          |List of strings |Yes               |N/A              |A list of ISO intervals to be indexed. ALL data for these intervals MUST be present in `paths`|
|`maxRowsInMemory`    |positive integer|No                |`80000`          |Maximum number of rows to store in memory before an intermediate flush to disk|
|`targetPartitionSize`|positive integer|No                |`5000000`        |The target number of rows per partition per segment granularity|
|`master`             |String          |No                |`master[1]`      |The spark master URI|
|`properties`         |Map             |No                |none             | A map of string key/value pairs to inject into the SparkContext properties overriding any prior set values|
|`id`                 |String          |No                |Assigned based on `dataSource`, `intervals`, `and DateTime.now()`|The ID for the task. If not provied it will be assigned|
|`indexSpec`          |InputSpec       |No                |concise, lz4, lz4|The InputSpec containing the various compressions to be used|
|`context`            |Map             |No                |none             |The task context|
