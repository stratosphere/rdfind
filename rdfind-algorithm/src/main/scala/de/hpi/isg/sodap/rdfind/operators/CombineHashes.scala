package de.hpi.isg.sodap.rdfind.operators

import de.hpi.isg.sodap.util.gp.CollectionUtils
import org.apache.flink.api.common.functions.ReduceFunction

/**
 * @author sebastian.kruse 
 * @since 18.05.2015
 */
class CombineHashes extends ReduceFunction[(String, Array[String])] {

  lazy val collector = new java.util.ArrayList[String]()

  override def reduce(first: (String, Array[String]), second: (String, Array[String])): (String, Array[String]) = {
    // It is very likely that one string set is included in the other one.
    var firstStrings = first._2
    var secondStrings = second._2
    if (CollectionUtils.intersectionSize(firstStrings, secondStrings) == Math.min(firstStrings.length, secondStrings.length)) {
      return if (firstStrings.length > secondStrings.length) first else second
    }

    // In rather rare cases, we have to calculate the union of two strings.
    this.collector.clear()
    CollectionUtils.unionAll(this.collector, firstStrings, secondStrings)
    return (first._1, this.collector.toArray(new Array[String](this.collector.size)))
  }
}
