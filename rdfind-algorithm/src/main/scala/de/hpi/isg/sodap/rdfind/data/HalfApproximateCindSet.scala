package de.hpi.isg.sodap.rdfind.data

import com.google.common.hash.BloomFilter

import scala.runtime.ScalaRunTime

/**
 * @author sebastian.kruse
 */
case class HalfApproximateCindSet(var depCaptureType: Int,
                                  var depConditionValue1: String,
                                  var depConditionValue2: String,
                                  var depCount: Int,
                                  var approximateRefConditions: Array[Long],
                                  var refConditions: Array[Condition],
                                  var isExact: Boolean)
extends CandidateSet {

  def toCindSet: CindSet = {
    if (!isExact) throw new IllegalStateException(s"Cannot split non-exact $this.")
    CindSet(depCaptureType, depConditionValue1, depConditionValue2, depCount, refConditions)
  }

  override def toString: String = s"${getClass.getSimpleName}(${Condition(depConditionValue1,depConditionValue2,depCaptureType)},${depCount}x,exact:$isExact,${if(approximateRefConditions == null) ScalaRunTime.stringOf(refConditions) else approximateRefConditions})"

  override def depCondition: Condition = Condition(depConditionValue1, depConditionValue2, depCaptureType)
}
