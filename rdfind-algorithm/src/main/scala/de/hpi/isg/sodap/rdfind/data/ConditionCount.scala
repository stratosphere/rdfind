package de.hpi.isg.sodap.rdfind.data

import com.google.common.hash.{PrimitiveSink, Funnel}
import de.hpi.isg.sodap.rdfind.util.{ConditionCodes, NullSensitiveOrdered}

/**
 * This class represents the number of occurrences of a condition or the number of (distinct) values of a capture.
 * Their order is exactly the same as for [[Condition]], i.e., the count is not regarded.
 *
 * @author sebastian.kruse 
 * @since 08.05.2015
 */
case class ConditionCount(var captureType: Int,
                          var conditionValue1: String,
                          var conditionValue2: String,
                          var count: Int)
  extends NullSensitiveOrdered[ConditionCount] {

  override def compare(that: ConditionCount): Int = {
    var result = Integer.compare(this.captureType, that.captureType)
    if (result != 0) return result

    result = compareNullSensitive(this.conditionValue1, that.conditionValue1)
    if (result != 0) return result

    compareNullSensitive(this.conditionValue2, that.conditionValue2)
  }

  def conditionValue1NotNull = coalesce(this.conditionValue1)

  def conditionValue2NotNull = coalesce(this.conditionValue2)

  private def coalesce(string: String) = if (string == null) "" else string

  override def toString: String = s"${count}x $toCondition"

  def toCondition = Condition(conditionValue1, conditionValue2, captureType)

  def toCondition(condition: Condition) = {
    condition.conditionType = this.captureType
    condition.conditionValue1 = this.conditionValue1
    condition.conditionValue2 = this.conditionValue2
    condition
  }


}

object ConditionCount {

  class NoCountFunnel extends Funnel[ConditionCount] {
    override def funnel(from: ConditionCount, into: PrimitiveSink): Unit =
      into
        .putInt(from.captureType)
        .putUnencodedChars(from.conditionValue1NotNull)
        .putUnencodedChars(from.conditionValue2NotNull)

    override def equals(obj: scala.Any): Boolean = obj != null && obj.isInstanceOf[NoCountFunnel]
  }

  object CountOrder extends Ordering[ConditionCount] {
    override def compare(x: ConditionCount, y: ConditionCount): Int = x.count compare y.count
  }
}