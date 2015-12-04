# druid-spark-batch
Druid indexing plugin for using Spark in batch jobs

This repository holds a Druid extension for using Spark as the engine for running batch jobs

To build issue the commnand `JAVA_HOME=$(/usr/libexec/java_home -v 1.7) sbt clean test publish-local publish-m2`

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

To use this extension, the hadoop client libraries and spark assembly (which might include the hadoop libraries) should be on the classpath of the overlord or middle manager. And the following should be added to `druid.extensions.coordinates`  : `io.druid.extensions:druid-spark-batch_2.10:jar:assembly:0.0.13` (obviously with the corrected version for whichever version you are using)
