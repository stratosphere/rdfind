package de.hpi.isg.sodap.rdfind.operators.candidate_merging

import com.google.common.hash.BloomFilter
import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.util.BloomFilterParameters
import org.apache.flink.api.common.functions.RichGroupReduceFunction.Combinable
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * @author sebastian.kruse 
 * @since 05.05.2015
 */
// Not combinable because it fails irregularly causing out of memory exceptions when allocating some array.
@Combinable
class IntersectHalfApproximateCindCandidates(bloomFilterParameters: BloomFilterParameters[Condition], windowSize: Int = -1)
  extends BulkMergeDependencies[HalfApproximateCindSet, Condition](windowSize) {

  private var mergeGroupSize: Int = _

  private var mergeGroupMember: Condition = _

  private var isAllInputExact = true

  private lazy val emptyConditionArray = new Array[Condition](0)

  private var fuzzyReferencedConditions: BloomFilter[Condition] = _

  override protected def priorityQueueOrdering: Ordering[(Condition, Int)] =
    BulkMergeDependencies.ConditionPriorityQueueOrdering

  /**
   * Add the given candidates to the window in this iteration.
   */
  override def addToWindow(candidates: HalfApproximateCindSet): Unit = {
    if (candidates.approximateRefConditions.length == 0) {
      this.exactWindow += candidates.refConditions
    } else {
      val approximateRefConditions = bloomFilterParameters.createBloomFilter
      approximateRefConditions.wrap(candidates.approximateRefConditions)
      if (this.fuzzyReferencedConditions == null) this.fuzzyReferencedConditions = approximateRefConditions
      else this.fuzzyReferencedConditions.intersect(approximateRefConditions)
    }
    this.isAllInputExact &= candidates.isExact
  }

  /** Tells the number of elements that must still be mergeable in the window. */
  override def minCandidateSize: Int = this.exactWindow.size

  override def prepareForMergeGroup(): Unit = {
      this.mergeGroupSize = 0
      this.mergeGroupMember = null
  }

  override def noticeMergeGroupMember(member: Condition): Unit = {
      this.mergeGroupSize += 1
      this.mergeGroupMember = member
  }

  override def addMergedGroup(collector: mutable.Buffer[Condition]): Unit =
    if (this.mergeGroupSize == this.exactWindow.size) {
      collector += this.mergeGroupMember
    }


  override def outputResults(out: Collector[HalfApproximateCindSet], depCondition: Condition, depCount: Int): Unit = {
    // Extract the exact referenced conditions if available.
    var exactRefConditions = if (this.exactWindow.nonEmpty) this.exactWindow(0) else null

    // Merge exact referenced conditions and Bloom filter, if necessary.
    if (exactRefConditions != null && this.fuzzyReferencedConditions != null) {
      exactRefConditions = exactRefConditions.filter { refCondition =>
        this.fuzzyReferencedConditions.mightContain(refCondition)
      }
      this.fuzzyReferencedConditions = null

    } else if (exactRefConditions == null
      && this.fuzzyReferencedConditions != null
      && this.fuzzyReferencedConditions.bitCount == 0) {
      // Note: Instead of 0, we could test if the bit count is smaller than the number of hash functions applied in
      // the Bloom filter, but this probably helps only in very few cases.

      this.fuzzyReferencedConditions = null
      exactRefConditions = emptyConditionArray
    }

    // Find out, if our result is exact.
    val isResultExact = this.isAllInputExact || (exactRefConditions != null && exactRefConditions.length == 0)

    // Output the CIND set.
    out.collect(HalfApproximateCindSet(depCondition.conditionType,
      depCondition.conditionValue1NotNull,
      depCondition.conditionValue2NotNull,
      depCount,
      if (this.fuzzyReferencedConditions == null) Array() else this.fuzzyReferencedConditions.exportBits(),
      if (exactRefConditions == null) emptyConditionArray else exactRefConditions,
      isResultExact))
  }

  override def cleanUp(): Unit = {
    super.cleanUp()
    this.fuzzyReferencedConditions = null
    this.isAllInputExact = true
  }

}




