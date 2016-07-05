package de.hpi.isg.sodap.rdfind.data

import de.hpi.isg.sodap.rdfind.util.ConditionCodes

/**
 * @author sebastian.kruse 
 * @since 15.04.2015
 */
case class SingleConditionCount(var conditionType: Int, var value: String, var count: Int) {

  override def toString: String = s"$count*${ConditionCodes.prettyPrint(conditionType,value)}"

}

object SingleConditionCount {

  def lessThan(thiss: SingleConditionCount, that: SingleConditionCount): Boolean = {
    return compare(thiss, that) < 0
  }

  def compare(thiss: SingleConditionCount, that: SingleConditionCount): Int = {
    val typeResult = Integer.compare(thiss.conditionType, that.conditionType)
    if (typeResult == 0) {
      return thiss.value.compareTo(that.value)
    }
    typeResult
  }

}