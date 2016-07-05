package de.hpi.isg.sodap.rdfind.data

import com.google.common.hash.{Funnel => _Funnel, PrimitiveSink}
import de.hpi.isg.sodap.rdfind.util.{ConditionCodes, NullSensitiveOrdered}

/**
 * @author sebastian.kruse 
 * @since 15.04.2015
 */
case class Condition(var conditionValue1: String, var conditionValue2: String, var conditionType: Int)
  extends NullSensitiveOrdered[Condition] {

  def checkSanity() = {
    if (!ConditionCodes.isValidStandardCapture(this.conditionType)) {
      throw new IllegalStateException(s"Illegal capture code ${this.conditionType} with ${(this.conditionValue1, this.conditionValue2)}.")
    }

    if (this.conditionValue1 == null || this.conditionValue1.isEmpty)
      throw new IllegalStateException(s"Missing first condition value  with ${(this.conditionValue1, this.conditionValue2)}.")

    if (this.conditionValue2 == null) {
      throw new IllegalStateException(s"Illegal second null value  with ${(this.conditionValue1, this.conditionValue2)}.")
    }

    if (ConditionCodes.isBinaryCondition(this.conditionType) && this.conditionValue1.isEmpty) {
      throw new IllegalStateException(s"Missing second condition value  with ${(this.conditionValue1, this.conditionValue2)}.")
    }
  }

  def toConditionCount(count: Int = 1): ConditionCount =
    ConditionCount(this.conditionType, this.conditionValue1, this.conditionValue2, count)


  /** Tells whether the other condition/captures is equal to or more general than this condition/capture. */
  def isImpliedBy(that: Condition): Boolean =
    this == that || (
      ConditionCodes.isSubcode(this.conditionType, that.conditionType) && this.conditionValue1 ==
        (if (ConditionCodes.extractFirstSubcapture(that.conditionType) == this.conditionType) {
          that.conditionValue1
        } else {
          that.conditionValue2
        })
      )

  def implies(that: Condition) = that.isImpliedBy(this)

  def conditionValue1NotNull = coalesce(this.conditionValue1)

  def conditionValue2NotNull = coalesce(this.conditionValue2)

  override def compare(that: Condition): Int = {
    var result = java.lang.Integer.compare(this.conditionType, that.conditionType)
    if (result != 0) return result

    result = compareNullSensitive(this.conditionValue1, that.conditionValue1)
    if (result != 0) return result

    compareNullSensitive(this.conditionValue2, that.conditionValue2)
  }

  def fillNullFields() = {
    if (this.conditionValue1 == null) this.conditionValue1 = ""
    if (this.conditionValue2 == null) this.conditionValue2 = ""
  }

  def isDoubleCondition = ConditionCodes.isBinaryCondition(this.conditionType)

  private def coalesce(string: String) = if (string == null) "" else string

  override def toString: String = ConditionCodes.prettyPrint(conditionType, conditionValue1, conditionValue2) + (if (conditionValue2 == "") "*" else "")

  def decoalesce() = {
    if (ConditionCodes.isUnaryCondition(this.conditionType)) this.conditionValue2 = null
    this
  }

  def coalesce() = {
    if (ConditionCodes.isUnaryCondition(this.conditionType)) this.conditionValue2 = ""
    this
  }

  def isUnary = ConditionCodes.isUnaryCondition(this.conditionType)

  def firstUnaryCondition = Condition(this.conditionValue1, null, ConditionCodes.extractFirstSubcapture(this.conditionType))

  def secondUnaryCondition = Condition(this.conditionValue2, null, ConditionCodes.extractSecondSubcapture(this.conditionType))

}

object Condition {

  /**
   * Funnel for [[Condition]]. Encodes only the values but not the count of a condition.
   */
  class Funnel extends _Funnel[Condition] {

    override def funnel(condition: Condition, primitiveSink: PrimitiveSink): Unit =
      primitiveSink
        .putUnencodedChars(condition.conditionValue1NotNull)
        .putUnencodedChars(condition.conditionValue2NotNull)
        .putInt(condition.conditionType)


    def canEqual(other: Any): Boolean = other.isInstanceOf[Funnel]

    override def equals(other: Any): Boolean = other match {
      case that: Funnel => true
      case _ => false
    }

    override def hashCode(): Int = 123456
  }
}
