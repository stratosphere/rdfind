package de.hpi.isg.sodap.rdfind.operators

import com.google.common.hash.data.IntArray
import de.hpi.isg.sodap.rdfind.data.{ConditionCount, HalfApproximateOverlapSet}
import de.hpi.isg.sodap.rdfind.util.BloomFilterParameters
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * @author sebastian.kruse 
 * @since 15.06.2015
 */
class EvaluateHalfApproximateOverlapSets(bloomFilterParameters: BloomFilterParameters[ConditionCount],
                                         minSupport: Int)
  extends RichFlatMapFunction[HalfApproximateOverlapSet, HalfApproximateOverlapSet] {

  var numFrequent = 0
  var numExactFrequent = 0
  var numInfrequent = 0
  var numUnknown = 0
  var numFrequentApproximate = 0

  lazy val exactOverlaps = new ArrayBuffer[ConditionCount]()
  lazy val approximateOverlaps = new ArrayBuffer[ConditionCount]()

  lazy val maxApproximateCount = bloomFilterParameters.createSpectralBloomFilter.getMaxValue

  lazy val output = HalfApproximateOverlapSet(0, null, null, 0, null, null)

  override def flatMap(halfApproximateOverlapSet: HalfApproximateOverlapSet, out: Collector[HalfApproximateOverlapSet]): Unit = {
    // Create a Bloom filter at first.
    val approximateCandidates =
      if (halfApproximateOverlapSet.approximateRhsConditions.length == 0)
        null
      else {
        val result = bloomFilterParameters.createSpectralBloomFilter
        result.wrap(halfApproximateOverlapSet.approximateRhsConditions)
        result
      }

    // Check all the explicit overlaps.
    halfApproximateOverlapSet.rhsConditions.foreach { rhsCondition =>
      val explicitCount = rhsCondition.count
      val approximateCount = if (approximateCandidates == null) 0 else approximateCandidates.getCount(rhsCondition)

      // If the explicit count is over the minimum support, we know that the given element is frequent.
      if (explicitCount >= minSupport) {
        if (approximateCount == 0) {
          this.numExactFrequent += 1
          this.exactOverlaps += rhsCondition
        } else {
          this.approximateOverlaps += rhsCondition
        }
        this.numFrequent += 1
      } else if (explicitCount + approximateCount < minSupport && approximateCount != this.maxApproximateCount) {
        this.numInfrequent += 1
      } else {
        this.approximateOverlaps += rhsCondition
        this.numUnknown += 1
      }
    }

    // Check if we need to look again for approximate overlap candidates.
    val recheckBloomFilterBits: Array[Long] =
      if (approximateCandidates != null) {
        val recheckBloomFilter = approximateCandidates.toBloomFilter(minSupport)
        if (recheckBloomFilter != null) {
          //        out.collect(s"Need to recheck approximate overlap candidates given by $recheckBloomFilter.")
          this.numFrequentApproximate += 1
          recheckBloomFilter.exportBits()
        } else {
          Array()
        }
      } else {
        Array()
      }


    val hasApproximateOverlaps = this.approximateOverlaps.nonEmpty || recheckBloomFilterBits.length > 0

    // We need to make sure that we also output overlaps if neither an exact or approximate overlap exists.
    if (this.exactOverlaps.nonEmpty || !hasApproximateOverlaps) {
      halfApproximateOverlapSet.rhsConditions = this.exactOverlaps.toArray
      halfApproximateOverlapSet.approximateRhsConditions = Array()
      out.collect(halfApproximateOverlapSet)
      this.exactOverlaps.clear()
    }

    if (hasApproximateOverlaps) {
      halfApproximateOverlapSet.rhsConditions =
        if (this.approximateOverlaps.nonEmpty) this.approximateOverlaps.toArray else Array()
      halfApproximateOverlapSet.approximateRhsConditions = recheckBloomFilterBits
      halfApproximateOverlapSet.lhsCount *= -1 // Mark the overlap set as approximate.
      out.collect(halfApproximateOverlapSet)
      this.approximateOverlaps.clear()
    }
  }

  override def close(): Unit = {
    super.close()

    val logger = LoggerFactory.getLogger(getClass)
    logger.info(s"Statistics for appoximate overlap evaluation " +
      s"(${getRuntimeContext.getIndexOfThisSubtask}/${getRuntimeContext.getNumberOfParallelSubtasks})")
    logger.info(s"Frequent:    $numFrequent (exact $numExactFrequent)")
    logger.info(s"Infrequent:  $numInfrequent")
    logger.info(s"Unknown:     $numUnknown")
    logger.info(s"Approximate: $numFrequentApproximate")
  }
}
