package de.hpi.isg.sodap.rdfind.operators

import java.util

import de.hpi.isg.sodap.rdfind.data.{JoinLine, JoinLineLoad}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * @author sebastian.kruse
 * @since 09.06.2015
 */
class AssignJoinLineRebalancing(parallelism: Int,
                                rebalanceFactor: Double,
                                maxLoad: Int,
                                extractMaximumJoinLineSize: (JoinLineLoad) => Int,
                                extractJoinLineSize: (JoinLine) => Int)
  extends RichFlatMapFunction[JoinLine, JoinLine] {

  lazy val logger = LoggerFactory.getLogger(getClass)

  if (parallelism == -1) throw new IllegalStateException("Parallelism must be set explicitly for load balancing.")

  private var maximumJoinLineSize: Int = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // Load average load join line load.
    val loads: util.List[JoinLineLoad] = getRuntimeContext.getBroadcastVariable[JoinLineLoad](
      AssignJoinLineRebalancing.AVERAGE_JOIN_LINE_LOAD_BROADCAST)
    // If the broadcast is empty, this means, there are no elements to rebalance.
    val averageJoinLineLoad = if (loads.isEmpty) {
      logger.warn("Could not find average join line load, defaulting to 0.")
      JoinLineLoad(0, 0, 0)
    } else loads.get(0)

    // Initialize the maximum size via the average join line load
    this.maximumJoinLineSize = Math.round(Math.max(extractMaximumJoinLineSize(averageJoinLineLoad) * rebalanceFactor, parallelism)).asInstanceOf[Int]
    // Cap this maximum size with the maximum load.
    val maxLoadSqrt = Math.round(Math.sqrt(this.maxLoad)).asInstanceOf[Int]
    this.maximumJoinLineSize = Math.min(maxLoadSqrt, this.maximumJoinLineSize)

  }

  override def flatMap(joinLine: JoinLine, out: Collector[JoinLine]): Unit = {
    // Let's take the combined criterion at first for rebalancing.
    val joinLineSize = extractJoinLineSize(joinLine)
    if (joinLineSize > maximumJoinLineSize) {
      val splitFactor = Math.ceil((joinLineSize * joinLineSize).asInstanceOf[Double] / (this.parallelism * maxLoad)).asInstanceOf[Int]
      val numSplits = splitFactor * this.parallelism
      logger.info("Rebalancing join line with {} relevant captures ({} splits).", joinLineSize, numSplits)
      joinLine.numPartitions = numSplits
      for (partitionKey <- 0 until numSplits) {
        joinLine.partitionKey = partitionKey
        out.collect(joinLine)
      }
    } else {
      joinLine.partitionKey = -1
      out.collect(joinLine)
    }
  }
}

object AssignJoinLineRebalancing {

  val AVERAGE_JOIN_LINE_LOAD_BROADCAST = "average-join-line-load"

}
