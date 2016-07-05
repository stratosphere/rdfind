package de.hpi.isg.sodap.rdfind.data

import de.hpi.isg.sodap.rdfind.util.ConditionCodes
import com.google.common.hash.{PrimitiveSink, Funnel => FunnerInterface}

/**
 * @author sebastian.kruse 
 * @since 15.04.2015
 */
case class DoubleConditionCount(var conditionType: Int,
                                var value1: String,
                                var value2: String,
                                var count: Int) extends Comparable[DoubleConditionCount] {

  def scrapCount = Condition(value1, value2, conditionType)

  override def compareTo(that: DoubleConditionCount): Int = {
    var result = Integer.compare(this.conditionType, that.conditionType)
    if (result != 0) return result

    result = this.value1.compareTo(that.value1)
    if (result != 0) return result

    return this.value2.compareTo(that.value2)
  }

  override def toString: String = s"$count*${ConditionCodes.prettyPrint(conditionType,value1, value2)}"
}

object DoubleConditionCount {

  class Funnel extends FunnerInterface[DoubleConditionCount] {

    override def funnel(t: DoubleConditionCount, primitiveSink: PrimitiveSink): Unit = {
      primitiveSink.putUnencodedChars(t.value1).putUnencodedChars(t.value2)
    }

    override def equals(o: scala.Any): Boolean = o != null && o.isInstanceOf[DoubleConditionCount.Funnel]
  }

  def lessThan(dcc1: DoubleConditionCount, dcc2: DoubleConditionCount) = dcc1.compareTo(dcc2) < 0
}