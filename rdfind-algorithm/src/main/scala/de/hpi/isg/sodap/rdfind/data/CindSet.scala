package de.hpi.isg.sodap.rdfind.data

import java.util

/**
 * @author sebastian.kruse 
 * @since 15.04.2015
 */
case class CindSet(var depCaptureType: Int,
                   var depConditionValue1: String,
                   var depConditionValue2: String,
                   var depCount: Int,
                   var refConditions: Array[Condition])
extends CandidateSet {

  def update(depCondition: Condition): Unit = {
    this.depCaptureType = depCondition.conditionType
    this.depConditionValue1 = depCondition.conditionValue1NotNull
    this.depConditionValue2 = depCondition.conditionValue2NotNull
  }

  override def toString: String = s"cindSet($depCondition < " +
    s"${util.Arrays.toString(refConditions.asInstanceOf[Array[AnyRef]])}"

  def split = this.refConditions.map { refCondition =>
    Cind(this.depCaptureType, this.depConditionValue1, this.depConditionValue2,
      refCondition.conditionType, refCondition.conditionValue1NotNull, refCondition.conditionValue2NotNull,
      this.depCount)
  }

  def depCondition = Condition(this.depConditionValue1, this.depConditionValue2, this.depCaptureType)
}
