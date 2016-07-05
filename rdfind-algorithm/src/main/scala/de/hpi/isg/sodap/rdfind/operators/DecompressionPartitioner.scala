package de.hpi.isg.sodap.rdfind.operators

import org.apache.flink.api.common.functions.Partitioner

/**
 * This partitioner round-robin partitions empty strings and hash-parititions all other strings. This is useful
 * co-groups and joins where we know that those empty strings will not find a join partner.
 *
 * @author Sebastian
 * @since 24.06.2015.
 */
class DecompressionPartitioner extends Partitioner[String] {

  var nextRandomPartition = scala.util.Random.nextInt(1000)

  override def partition(key: String, numPartitions: Int): Int = {
    if (key == null || key.isEmpty) {
      this.nextRandomPartition += 1
      this.nextRandomPartition %= numPartitions
      this.nextRandomPartition
    } else {
      Math.abs(key.hashCode) % numPartitions
    }
  }

}
