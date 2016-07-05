package de.hpi.isg.sodap.rdfind.operators.candidate_merging

import java.lang.Iterable

import de.hpi.isg.sodap.rdfind.data.{Condition, ConditionCount, OverlapSet}
import org.apache.flink.api.common.functions.RichGroupReduceFunction.Combinable
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author sebastian.kruse 
 * @since 05.05.2015
 */
@Combinable
class MultiunionOverlapCandidates(windowSize: Int = -1)
  extends BulkMergeDependencies[OverlapSet, ConditionCount](windowSize) {

  private var mergeGroupAggregator: ConditionCount = _

  override protected def priorityQueueOrdering: Ordering[(ConditionCount, Int)] =
    BulkMergeDependencies.ConditionCountPriorityQueueOrdering

  /**
   * Add the given candidates to the window in this iteration.
   */
  override def addToWindow(candidates: OverlapSet): Unit = this.exactWindow += candidates.rhsConditions

  override def prepareForMergeGroup(): Unit = this.mergeGroupAggregator = null

  override def noticeMergeGroupMember(member: ConditionCount): Unit =
    if (this.mergeGroupAggregator == null) {
      this.mergeGroupAggregator = member
    } else {
      this.mergeGroupAggregator.count += member.count
    }

  override def addMergedGroup(collector: mutable.Buffer[ConditionCount]): Unit =
    collector += this.mergeGroupAggregator

  override def outputResults(out: Collector[OverlapSet], depCondition: Condition, depCount: Int): Unit =
    out.collect(
      OverlapSet(depCondition.conditionType, depCondition.conditionValue1NotNull, depCondition.conditionValue2NotNull,
        depCount, this.exactWindow(0)))

  /** Tells the number of elements that must still be mergeable in the window. */
  override def minCandidateSize: Int = 1

}


