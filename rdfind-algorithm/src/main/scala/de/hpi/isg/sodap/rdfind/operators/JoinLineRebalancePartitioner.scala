package de.hpi.isg.sodap.rdfind.operators

import org.apache.flink.api.common.functions.Partitioner

import scala.util.Random

/**
 * @author sebastian.kruse 
 * @since 09.06.2015
 */
class JoinLineRebalancePartitioner extends Partitioner[Int] {

  lazy val random = new Random()

  override def partition(key: Int, numPartitions: Int): Int =
    if (key == -1)
      this.random.nextInt(numPartitions)
    else
      key % numPartitions

}
