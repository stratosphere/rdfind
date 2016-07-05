package de.hpi.isg.sodap.rdfind.operators

import de.hpi.isg.sodap.rdfind.data.{Condition, JoinLine}
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields

import scala.collection.mutable

/**
 * @author sebastian.kruse 
 * @since 09.06.2015
 */
//@ForwardedFields(Array("conditions -> conditions"))
class AnnotateJoinLineSizes extends RichMapFunction[JoinLine, JoinLine]{

  lazy val unaryConditions = mutable.HashSet[Condition]()

  override def map(joinLine: JoinLine): JoinLine = {
    // Count the known-to-be distinct binary captures and collect the non-distinct unary captures.
    var numBinaryCaptures = 0
    joinLine.conditions.foreach { capture =>
      if (capture.isDoubleCondition) {
        numBinaryCaptures += 1
        val conditions = decodeConditionCode(capture.conditionType, isRequireDoubleCode = true)
        val newConditionCode1 = createConditionCode(conditions._1, secondaryCondition = conditions._3)
        this.unaryConditions += Condition(capture.conditionValue1, null, newConditionCode1)
        val newConditionCode2 = createConditionCode(conditions._2, secondaryCondition = conditions._3)
        this.unaryConditions += Condition(capture.conditionValue2, null, newConditionCode2)
      } else {
        this.unaryConditions += capture
      }
    }

    // Output the numbers of distinct unary and binary captures.
    joinLine.numUnaryConditions = this.unaryConditions.size
    joinLine.numBinaryConditions = numBinaryCaptures
    this.unaryConditions.clear()

    joinLine
  }

  def splitAndCollectUnaryCaptures(collector: mutable.Set[Condition], condition: Condition): Unit = {
    val conditions = decodeConditionCode(condition.conditionType, isRequireDoubleCode = true)
    val newConditionCode1 = createConditionCode(conditions._1, secondaryCondition = conditions._3)
    collector += Condition(condition.conditionValue1, null, newConditionCode1)
    val newConditionCode2 = createConditionCode(conditions._2, secondaryCondition = conditions._3)
    collector += Condition(condition.conditionValue2, null, newConditionCode2)
  }
}
