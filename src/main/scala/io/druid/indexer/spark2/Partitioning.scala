package io.druid.indexer.spark2

import com.metamx.common.logger.Logger
import com.metamx.common.{IAE, ISE, Granularity}
import io.druid.segment.indexing.DataSchema
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Interval}

import scala.collection.JavaConversions._

case class DruidPartitionInfo(timeBucket : Long,
                              partitionNum : Int,
                              interval : Interval,
                              count : Int)

case class DruidPartitionIndexMap(val hashToPartitionMap: Map[(Long, Long), Int]) {

  lazy val maxTimePerBucket = hashToPartitionMap.groupBy(_._1._1).map(e => e._1 -> e._2.size)

  def numPartitions: Int = hashToPartitionMap.size

  def dumpToLog(implicit log : Logger) : Unit = {
    hashToPartitionMap.foreach {
      case((bucket, pNum), idx) =>
        log.info("Date Bucket [%s] with partition number [%s] has total partition number [%s]",
          Array(bucket, new java.lang.Long(pNum), new Integer(idx))
        )
    }
  }

  def partitionNums(index : Int) = {
    val partitionNums = hashToPartitionMap.filter(_._2 == index).map(_._1._2.toInt).toSeq

    if (partitionNums.isEmpty) {
      throw new ISE(s"Partition [$index] not found in partition map. " +
        s"Valid parts: ${hashToPartitionMap.values}"
      )
    }
    if (partitionNums.length != 1) {
      throw new ISE(s"Too many partitions found for index [$index] Found $partitionNums")
    }

    partitionNums
  }

  def apply(index : Int)
           (implicit dataSchema : DataSchema, ingestInterval: Interval): DruidPartitionInfo = {

    val partitionNums = hashToPartitionMap.filter(_._2 == index).map(_._1._2.toInt).toSeq

    if (partitionNums.isEmpty) {
      throw new ISE(s"Partition [$index] not found in partition map. " +
        s"Valid parts: ${hashToPartitionMap.values}"
      )
    }
    if (partitionNums.length != 1) {
      throw new ISE(s"Too many partitions found for index [$index] Found $partitionNums")
    }

    val pNum = partitionNums.head
    val tBucket = hashToPartitionMap.filter(_._2 == index).map(_._1._1).head

    DruidPartitionInfo(tBucket,
      pNum,
      dataSchema.getGranularitySpec.getSegmentGranularity.getIterable(ingestInterval)
        .toSeq.filter(_.getStart.getMillis == tBucket).head,
      hashToPartitionMap.count(_._1._1 == tBucket)
    )
  }

  def getOrElse(key: (Long, Long), default: => Nothing) = hashToPartitionMap.getOrElse(key, default)
}

object DruidPartitionIndexMap {

  def apply(baseData : RDD[GranularizedDruidRow], rowsPerPartition: Long)
           (implicit dataSchema : SerializedJson[DataSchema], ingestInterval: Interval) : DruidPartitionIndexMap = {
    val partitionMap: Map[Long, Long] = baseData
      .countApproxDistinctByKey(
        0.05,
        new DateBucketPartitioner(dataSchema.getDelegate.getGranularitySpec.getSegmentGranularity,
          ingestInterval)
      )
      .map(x => dataSchema.getDelegate.getGranularitySpec.getSegmentGranularity.truncate(
        new DateTime(x._1)).getMillis -> x._2
      )
      .reduceByKey(_ + _)
      .collect().toMap

    new DruidPartitionIndexMap(getSizedPartitionMap(partitionMap, rowsPerPartition))
  }

  /**
    * Take a map of indices and size for that index, and return a map of (index, sub_index)->new_index
    * each new_index of which can have at most rowsPerPartition assuming random-ish hashing into the indices
    * @param inMap A map of index to count of items in that index
    * @param rowsPerPartition The maximum desired size per output index.
    * @return A map of (index, sub_index)->new_index . The size of this map times rowsPerPartition is greater than
    *         or equal to the number of events (sum of keys of inMap)
    */
  private def getSizedPartitionMap(inMap: Map[Long, Long], rowsPerPartition: Long):
  Map[(Long, Long), Int] = {
    var sz = -1
    for( ((dateRangeBucket, numEventsInRange), i) <- inMap.filter(_._2 > 0).zipWithIndex;
                  subIndex <- Range.apply(0, (numEventsInRange / rowsPerPartition + 1).toInt)
    ) yield { sz+=1; (dateRangeBucket, subIndex.toLong) -> sz}
  }
}


class DateBucketPartitioner(gran: Granularity, interval: Interval) extends Partitioner
{
  val intervalMap: Map[Long, Int] = gran.getIterable(interval)
    .toSeq
    .map(_.getStart.getMillis)
    .foldLeft(Map[Long, Int]())((a, b) => a + (b -> a.size))

  override def numPartitions: Int = intervalMap.size

  override def getPartition(key: Any): Int = key match {
    case null => throw new NullPointerException("Bad partition key")
    case (k: Long, v: Any) => getPartition(k)
    case (k: Long) =>
      val mapKey = gran.bucket(new DateTime(k)).getStart.getMillis
      val v = intervalMap.get(mapKey)
      if (v.isEmpty) {
        // Lazy ISE creation. getOrElse will create it each time
        throw new ISE(
          "%s",
          "unknown bucket for datetime %s. Known values are %s" format(mapKey, intervalMap.keys)
        )
      }
      v.get
    case x => throw new IAE("%s", "Unknown type for %s" format x)
  }
}

class DateBucketAndHashPartitioner(gran: Granularity, interval: Interval,
                                   druidPartMap: DruidPartitionIndexMap)
  extends Partitioner
{

  override def numPartitions: Int = druidPartMap.numPartitions

  override def getPartition(key: Any): Int = key match {
    case (k: Long, v: AnyRef) =>
      val dateBucket = gran.truncate(new DateTime(k)).getMillis
      val modSize = druidPartMap.maxTimePerBucket.get(dateBucket.toLong)
      if(modSize.isEmpty) {
        // Lazy ISE creation
        throw new ISE("%s", "bad date bucket [%s]. available: %s"
          format(dateBucket.toLong, druidPartMap.maxTimePerBucket.keySet))
      }
      val hash = Math.abs(v.hashCode()) % modSize.get
      druidPartMap
        .getOrElse(
          (dateBucket.toLong, hash.toLong), {
            throw new ISE("bad hash and bucket combo: (%s, %s)" format(dateBucket, hash))
          }
        )
    case x => throw new IAE("%s", "Unknown type [%s]" format x)
  }
}
