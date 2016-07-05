package de.hpi.isg.sodap.rdfind.operators

import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * @author sebastian.kruse
 * @since 08.05.2015
 */
class CreateUnaryUnaryOverlapCandidates(isUsingAssociationRules: Boolean)
  extends CreateDependencyCandidates[OverlapSet, ConditionCount, Unit](true, false, isUsingAssociationRules) {

  if (isUsingAssociationRules)
    throw new IllegalArgumentException(s"${getClass.getSimpleName} does not support association rules.")

  override protected def createUnaryConditions: mutable.Set[ConditionCount] = mutable.SortedSet()

  lazy val output = OverlapSet(0, null, null, 0, null)

  override def collectBinaryCaptures(collector: mutable.Set[Unit], condition: Condition): Unit = ???

  override protected def createBinaryConditions: mutable.Set[Unit] = ???

  override protected def collectUnaryCapture(collector: mutable.Set[ConditionCount], condition: Condition): Unit =
    collector += condition.toConditionCount()

  override def splitAndCollectUnaryCaptures(collector: mutable.Set[ConditionCount], condition: Condition): Unit = {
    val conditions = decodeConditionCode(condition.conditionType, isRequireDoubleCode = true)
    val newConditionCode1 = createConditionCode(conditions._1, secondaryCondition = conditions._3)
    collector += ConditionCount(newConditionCode1, condition.conditionValue1, null, 1)
    val newConditionCode2 = createConditionCode(conditions._2, secondaryCondition = conditions._3)
    collector += ConditionCount(newConditionCode2, condition.conditionValue2, null, 1)
  }


  // We do not need this function as we override its more specific sibling.
  override def collectDependencyCandidates(unaryConditions: mutable.Set[ConditionCount],
                                           binaryConditions: mutable.Set[Unit],
                                           out: Collector[OverlapSet]): Unit = ???

  override def collectDependencyCandidates(unaryConditions: mutable.Set[ConditionCount],
                                           binaryConditions: mutable.Set[Unit],
                                           joinLine: JoinLine,
                                           out: Collector[OverlapSet]): Unit = {

    // We shrink the RHS constantly so as to guaruantee to have each pair of conditions only once.
    var lastRhs: Array[ConditionCount] = null
    // If we skip some RHS due to load-balancing, we need to keep track of how many we skipped.
    var distance = 1
    unaryConditions.foreach { lhsConditionCount =>
      if (shouldProcess(joinLine, lhsConditionCount)) {
        output.lhsCaptureType = lhsConditionCount.captureType
        output.lhsConditionValue1 = lhsConditionCount.conditionValue1NotNull
        output.lhsConditionValue2 = lhsConditionCount.conditionValue2NotNull
        output.lhsCount = 1
        if (lastRhs == null) {
          lastRhs = new Array[ConditionCount](unaryConditions.size)
          unaryConditions.copyToArray(lastRhs)
        }
        val rhs: Array[ConditionCount] = new Array[ConditionCount](lastRhs.length - distance)
        System.arraycopy(lastRhs, distance, rhs, 0, rhs.length)
        output.rhsConditions = rhs
        lastRhs = rhs

        out.collect(output)
        distance = 1
      } else {
        distance += 1
      }
    }
  }
}
