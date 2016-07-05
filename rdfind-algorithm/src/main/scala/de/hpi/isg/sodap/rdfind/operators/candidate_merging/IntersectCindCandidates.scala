package de.hpi.isg.sodap.rdfind.operators.candidate_merging

import de.hpi.isg.sodap.rdfind.data.{CindSet, Condition}
import org.apache.flink.api.common.functions.RichGroupReduceFunction.Combinable
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * @author sebastian.kruse 
 * @since 05.05.2015
 */
@Combinable
class IntersectCindCandidates(windowSize: Int = -1,
                              countMergeFunction: (Int, Int) => Int = _ + _)
  extends BulkMergeDependencies[CindSet, Condition](windowSize, countMergeFunction) {

  private var mergeGroupSize: Int = _

  private var mergeGroupMember: Condition = _

  override protected def priorityQueueOrdering: Ordering[(Condition, Int)] =
    BulkMergeDependencies.ConditionPriorityQueueOrdering

  /**
   * Add the given candidates to the window in this iteration.
   */
  override def addToWindow(candidates: CindSet): Unit = this.exactWindow += candidates.refConditions

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

  override def outputResults(out: Collector[CindSet], depCondition: Condition, depCount: Int): Unit =
    out.collect(CindSet(depCondition.conditionType, depCondition.conditionValue1NotNull, depCondition.conditionValue2NotNull,
      depCount, this.exactWindow(0)))

  /** Tells the number of elements that must still be mergeable in the window. */
  override def minCandidateSize: Int = this.exactWindow.size

}
