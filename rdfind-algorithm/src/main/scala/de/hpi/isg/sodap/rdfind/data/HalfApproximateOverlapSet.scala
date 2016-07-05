package de.hpi.isg.sodap.rdfind.data

import scala.runtime.ScalaRunTime

/**
 * @author sebastian.kruse 
 * @since 15.04.2015
 */
case class HalfApproximateOverlapSet(var lhsCaptureType: Int,
                                     var lhsConditionValue1: String,
                                     var lhsConditionValue2: String,
                                     var lhsCount: Int,
                                     var rhsConditions: Array[ConditionCount],
                                     var approximateRhsConditions: Array[Long])
  extends CandidateSet {

  def toOverlapSet: OverlapSet = OverlapSet(lhsCaptureType, lhsConditionValue1, lhsConditionValue2, lhsCount, rhsConditions)

  def update(depCondition: Condition): Unit = {
    this.lhsCaptureType = depCondition.conditionType
    this.lhsConditionValue1 = depCondition.conditionValue1NotNull
    this.lhsConditionValue2 = depCondition.conditionValue2NotNull
  }

  def update(depConditionCount: ConditionCount): Unit = {
    this.lhsCaptureType = depConditionCount.captureType
    this.lhsConditionValue1 = depConditionCount.conditionValue1NotNull
    this.lhsConditionValue2 = depConditionCount.conditionValue2NotNull
    this.lhsCount = depConditionCount.count
  }

  override def depCount: Int = this.lhsCount

  override def depCondition: Condition =
    Condition(this.lhsConditionValue1, this.lhsConditionValue2, this.lhsCaptureType)

  override def toString: String = s"${getClass.getSimpleName}(${lhsCount}x ${Condition(lhsConditionValue1, lhsConditionValue2, lhsCaptureType)}->${ScalaRunTime.stringOf(rhsConditions)},${ScalaRunTime.stringOf(approximateRhsConditions)})"
}
