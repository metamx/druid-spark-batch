package io.druid.indexer.spark2

import io.druid.indexer.spark.SparkBatchIndexTask
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

class LocalSQLContext(sc: SparkContext) extends SQLContext(sc) { self =>

  def this() {
    this(new SparkContext("local[*]", "test-sql-context",
      new SparkConf().set("spark.sql.testkey", "true")))
  }

  def this(sConf : SparkConf) {
    this(new SparkContext(sConf))
  }

}

object TestSQLContext extends LocalSQLContext(new SparkConf()
  .setAppName("Simple Application")
  .setMaster("local[4]")
  .set("user.timezone", "UTC")
  .set("file.encoding", "UTF-8")
  .set("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager")
  .set("org.jboss.logging.provider", "slf4j")
  .set("druid.processing.columnCache.sizeBytes", "1000000000")
  .set("spark.driver.host", "localhost")
  .set("spark.executor.userClassPathFirst", "true")
  .set("spark.driver.userClassPathFirst", "true")
  .set("spark.kryo.referenceTracking", "false")
  .registerKryoClasses(SparkBatchIndexTask.KRYO_CLASSES))