package de.hpi.isg.sodap.rdfind.operators

import de.hpi.isg.sodap.rdfind.data.{Condition, JoinLine}
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import org.apache.flink.api.common.functions.{RichFilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * @author sebastian.kruse 
 * @since 03.07.2015
 */
@deprecated(message = "This is only a debug function and should not be used in production.")
class PrintInfrequentCaptureSavings extends RichFilterFunction[JoinLine] {

  var infrequentConditions: Set[Condition] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val broadcast = getRuntimeContext.getBroadcastVariable[Condition]("infrequent-conditions")
    broadcast.foreach(_.decoalesce())
    this.infrequentConditions = broadcast.toSet
  }

  override def filter(joinLine: JoinLine): Boolean = {
    // Materialize conditions.
    val conditions = new mutable.HashSet[Condition]()
    joinLine.conditions.foreach { condition =>
      conditions += condition
      if (!condition.isUnary) {
        val conditionCodes = decodeConditionCode(condition.conditionType, isRequireDoubleCode = true)
        val newConditionCode1 = createConditionCode(conditionCodes._1, secondaryCondition = conditionCodes._3)
        conditions += Condition(condition.conditionValue1, "", newConditionCode1)
        val newConditionCode2 = createConditionCode(conditionCodes._2, secondaryCondition = conditionCodes._3)
        conditions += Condition(condition.conditionValue2, "", newConditionCode2)
      }
    }

    // Count the number condition to be removed.
    val overallConditions = conditions.size
    val unnecessaryConditions = conditions.intersect(this.infrequentConditions).size

    if (unnecessaryConditions > 0) {
      println(s"Can reduce condtions in join line from $overallConditions by $unnecessaryConditions")
      if (unnecessaryConditions > 5) {
        println(s"Namely remove ${conditions.intersect(this.infrequentConditions)} by $conditions")
      }
    }

    true
  }
}
