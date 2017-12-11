package io.druid.indexer.spark

import java.util

import com.google.common.base.Joiner
import io.druid.java.util.common.DateTimes
import org.joda.time.Interval

object TaskIdGenerator {
  private val ID_JOINER = Joiner.on("_")

  def getOrMakeId(id: String, typeName: String, dataSource: String, interval: Interval): String = {
    if (id != null) return id
    val objects = new util.ArrayList[AnyRef]
    objects.add(typeName)
    objects.add(dataSource)
    if (interval != null) {
      objects.add(interval.getStart)
      objects.add(interval.getEnd)
    }
    objects.add(DateTimes.nowUtc.toString)
    ID_JOINER.join(objects)
  }
}
