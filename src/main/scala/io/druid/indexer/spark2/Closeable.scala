package io.druid.indexer.spark2

import java.io
import java.io.File

import com.google.common.io.Closer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.commons.io.FileUtils

trait Closeable[T] extends java.io.Closeable {
  self =>

  val obj : T

  def registerForClose(implicit closer : Closer) : T = {
    closer.register(new io.Closeable {
      override def close(): Unit = self.close
    })
    obj
  }
}

object Closeable {

  implicit def sparkContextToCloseable(sc : SparkContext) = new Closeable[SparkContext] {
    val obj = sc
    override def close: Unit = sc.stop()
  }

  implicit def sparkSQLContextToCloseable(sqlContext : SQLContext) = new Closeable[SQLContext] {
    val obj = sqlContext
    override def close: Unit = sqlContext.sparkContext.stop()
  }

  implicit def fileToCloseable(file : File) = new Closeable[File] {
    val obj = file
    override def close: Unit = FileUtils.deleteDirectory(file)
  }

  implicit def jCloseableToCloseable[U <: java.io.Closeable](closeable : U) = new Closeable[U] {
    val obj = closeable
    override def close: Unit = closeable.close()
  }

}