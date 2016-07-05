package de.hpi.isg.sodap.rdfind.operators.candidate_merging

import com.google.common.hash.SpectralBloomFilter
import de.hpi.isg.sodap.rdfind.data.{Condition, ConditionCount, HalfApproximateOverlapSet}
import de.hpi.isg.sodap.rdfind.util.BloomFilterParameters
import org.apache.flink.api.common.functions.RichGroupReduceFunction.Combinable
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.util.Sorting

/**
 * @author sebastian.kruse 
 * @since 05.05.2015
 */
@Combinable
class MultiunionHalfApproximateOverlapCandidates(bloomFilterParameters: BloomFilterParameters[ConditionCount],
                                                 maxExplicitElements: Int,
                                                 windowSize: Int = -1)
  extends BulkMergeDependencies[HalfApproximateOverlapSet, ConditionCount](windowSize) {

  private var mergeGroupAggregator: ConditionCount = _

  private var bloomFilterAggregator: SpectralBloomFilter[ConditionCount] = _

  override protected def priorityQueueOrdering: Ordering[(ConditionCount, Int)] =
    BulkMergeDependencies.ConditionCountPriorityQueueOrdering

  /**
   * Add the given candidates to the window in this iteration.
   */
  override def addToWindow(candidates: HalfApproximateOverlapSet): Unit = {
    // Add the explicit part to the explicit window if given.
    if (candidates.rhsConditions.length > 0) {
      this.exactWindow += candidates.rhsConditions
    }

    // Add up the approximated part.
    if (candidates.approximateRhsConditions.length > 0) {
      val currentBloomFilter = this.bloomFilterParameters.createSpectralBloomFilter
      currentBloomFilter.wrap(candidates.approximateRhsConditions)
      if (this.bloomFilterAggregator == null) {
        this.bloomFilterAggregator = currentBloomFilter
      } else {
        this.bloomFilterAggregator.putAll(currentBloomFilter)
      }
    }
  }

  override def prepareForMergeGroup(): Unit = this.mergeGroupAggregator = null

  override def noticeMergeGroupMember(member: ConditionCount): Unit =
    if (this.mergeGroupAggregator == null) {
      this.mergeGroupAggregator = member
    } else {
      this.mergeGroupAggregator.count += member.count
    }

  override def addMergedGroup(collector: mutable.Buffer[ConditionCount]): Unit =
    collector += this.mergeGroupAggregator

  override def outputResults(out: Collector[HalfApproximateOverlapSet], depCondition: Condition, depCount: Int): Unit = {
    // Extract the explicit RHS counts if available.
    var explicitRhsCounts = if (this.exactWindow.nonEmpty) this.exactWindow(0) else null

    // Merge in excess explicit RHS counts if necessary.
    if (this.isCombining && explicitRhsCounts != null && explicitRhsCounts.length > maxExplicitElements) {
      val numExplicitElementsToRemove = explicitRhsCounts.length - maxExplicitElements
      Sorting.quickSort(explicitRhsCounts)(ConditionCount.CountOrder)

      // Take the first elements and add them to the Bloom filter.
      if (this.bloomFilterAggregator == null) {
        this.bloomFilterAggregator = bloomFilterParameters.createSpectralBloomFilter
      }
      for (i <- 0 until numExplicitElementsToRemove) {
        val rhsCount = explicitRhsCounts(i)
        this.bloomFilterAggregator.putToBagBatch(rhsCount, rhsCount.count)
      }
      this.bloomFilterAggregator.executeBagBatch()
      explicitRhsCounts = explicitRhsCounts.slice(numExplicitElementsToRemove, explicitRhsCounts.length)
      Sorting.quickSort(explicitRhsCounts)
    }

    // Extract the approximate RHS counts if available.
    val approximateRhsCounts = if (this.bloomFilterAggregator != null) this.bloomFilterAggregator else null

    out.collect(
      HalfApproximateOverlapSet(depCondition.conditionType,
        depCondition.conditionValue1NotNull,
        depCondition.conditionValue2NotNull,
        depCount,
        if (explicitRhsCounts == null) Array() else explicitRhsCounts,
        if (approximateRhsCounts == null) Array() else approximateRhsCounts.exportBits()))
  }


  override def cleanUp(): Unit = {
    super.cleanUp()
    this.bloomFilterAggregator = null
  }

  /** Tells the number of elements that must still be mergeable in the window. */
  override def minCandidateSize: Int = 1

}


