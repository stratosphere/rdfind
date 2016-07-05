package de.hpi.isg.sodap.rdfind.operators

import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * @author sebastian.kruse 
 * @since 08.05.2015
 */
trait CreateSymmetricDependencyCandidates[T, U] {

  def collectDependencyCandidates(conditions: mutable.Set[U], out: Collector[T]): Unit = ???
//    val referencedArray
//    if (conditions.isInstanceOf[mutable.SortedSet]) {
//
//    }
//    // Create overlap candidates for each of the single conditions.
//    val sortedConditions = allConditions.toArray.sortWith(SingleConditionCount.lessThan)
//    (0 until sortedConditions.length).foreach {
//      index =>
//        // TODO: Improve performance of this code.
//        val rhsConditions = util.Arrays.copyOfRange(sortedConditions, 0, index).toList ++
//          util.Arrays.copyOfRange(sortedConditions, index + 1, sortedConditions.length).toList
//        val lhsCondition = sortedConditions(index)
//        out.collect(SingleSingleOverlapSet(lhsCondition.conditionType, lhsCondition.value, 1, rhsConditions))
//    }

}
