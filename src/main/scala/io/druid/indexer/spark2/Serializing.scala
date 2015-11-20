package io.druid.indexer.spark2

import java.io._

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.inject.{Key, Binder, Module}
import com.metamx.common.logger.Logger
import io.druid.guice.annotations.{Smile, Self}
import io.druid.guice.{JsonConfigProvider, GuiceInjectors}
import io.druid.initialization.Initialization
import io.druid.server.DruidNode
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConversions._

/**
  * This is tricky. The type enforcing is only done at compile time. The JSON serde plays it fast and loose with the types
  */
@SerialVersionUID(713838456349L)
class SerializedJson[A](inputDelegate: A) extends KryoSerializable with Serializable
{
  @transient var delegate: A = inputDelegate

  @throws[IOException]
  private def writeObject(output: ObjectOutputStream) = {
    innerWrite(output)
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    innerWrite(output)
  }

  def innerWrite(output: OutputStream): Unit = SerializedJsonStatic.mapper
    .writeValue(SerializedJsonStatic.captureCloseOutputStream(output), toJavaMap)

  def toJavaMap = mapAsJavaMap(
    Map(
      "class" -> inputDelegate.getClass.getCanonicalName,
      "delegate" -> SerializedJsonStatic.mapper.writeValueAsString(delegate)
    )
  )

  def getMap(input: InputStream) = SerializedJsonStatic
    .mapper
    .readValue(SerializedJsonStatic.captureCloseInputStream(input), SerializedJsonStatic.mapTypeReference)
    .asInstanceOf[java.util.Map[String, Object]].toMap[String, Object]


  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readObject(input: ObjectInputStream) = {
    SerializedJsonStatic.log.trace("Reading Object")
    fillFromMap(getMap(input))
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    SerializedJsonStatic.log.trace("Reading Kryo")
    fillFromMap(getMap(input))
  }

  def fillFromMap(m: Map[String, Object]): Unit = {
    val clazzName: Class[_] = m.get("class") match {
      case Some(cn) => if (Thread.currentThread().getContextClassLoader == null) {
        SerializedJsonStatic.log.trace("Using class's classloader [%s]", getClass.getClassLoader)
        getClass.getClassLoader.loadClass(cn.toString)
      } else {
        SerializedJsonStatic.log.trace("Using context classloader [%s]", Thread.currentThread().getContextClassLoader)
        Thread.currentThread().getContextClassLoader.loadClass(cn.toString)
      }
      case _ => throw new NullPointerException("Missing `class`")
    }
    delegate = m.get("delegate") match {
      case Some(d) => SerializedJsonStatic.mapper.readValue(d.toString, clazzName).asInstanceOf[A]
      case _ => throw new NullPointerException("Missing `delegate`")
    }
    if (SerializedJsonStatic.log.isTraceEnabled) {
      SerializedJsonStatic.log.trace("Read in %s", delegate.toString)
    }
  }

  def getDelegate = delegate
}

object SerializedJsonStatic
{
  val log: Logger = new Logger(classOf[SerializedJson[Any]])

  val injector    = Initialization.makeInjectorWithModules(
    GuiceInjectors.makeStartupInjector(), List[Module](
      new Module
      {
        override def configure(binder: Binder): Unit = {
          JsonConfigProvider
            .bindInstance(
              binder,
              Key.get(classOf[DruidNode], classOf[Self]),
              new DruidNode("spark-indexer", null, null)
            )
        }
      }
    )
  )
  // I would use ObjectMapper.copy().configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false)
  // But https://github.com/FasterXML/jackson-databind/issues/696 isn't until 4.5.1, and anything 4.5 or greater breaks
  // EVERYTHING
  // So instead we have to capture the close
  val mapper      = injector.getInstance(Key.get(classOf[ObjectMapper], classOf[Smile]))

  def captureCloseOutputStream(ostream: OutputStream): OutputStream =
    new FilterOutputStream(ostream)
    {
      override def close(): Unit = {
        // Ignore
      }
    }

  def captureCloseInputStream(istream: InputStream): InputStream = new FilterInputStream(istream) {
    override def close(): Unit = {
      // Ignore
    }
  }

  val mapTypeReference = new TypeReference[java.util.Map[String, Object]] {}
}

@SerialVersionUID(68710585891L)
class SerializedHadoopConfig(delegate: Configuration) extends KryoSerializable with Serializable
{
  @transient var del = delegate

  @throws[IOException]
  private def writeObject(out: ObjectOutputStream) = {
    SerializedJsonStatic.log.trace("Writing Hadoop Object")
    del.write(out)
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readObject(in: ObjectInputStream) = {
    SerializedJsonStatic.log.trace("Reading Hadoop Object")
    del = new Configuration()
    del.readFields(in)
  }

  def getDelegate = del

  override def write(kryo: Kryo, output: Output): Unit = {
    SerializedJsonStatic.log.trace("Writing Hadoop Kryo")
    writeObject(new ObjectOutputStream(output))
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    SerializedJsonStatic.log.trace("Reading Hadoop Kryo")
    readObject(new ObjectInputStream(input))
  }
}
