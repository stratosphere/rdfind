package de.hpi.isg.sodap.rdfind.operators

import java.lang

import de.hpi.isg.sodap.rdfind.data.{AssociationRule, Cind}
import de.hpi.isg.sodap.rdfind.util.ConditionCodes
import org.apache.flink.api.common.functions.{BroadcastVariableInitializer, RichFilterFunction}
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConversions._

/**
 * @author sebastian.kruse 
 * @since 23.06.2015
 */
class FilterAssociationRuleImpliedCinds extends RichFilterFunction[Cind] {

  private var impliedCinds: Set[Cind] = _

  private lazy val testCind = Cind(0, null, "", 0, null, "", -1)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    this.impliedCinds = getRuntimeContext.getBroadcastVariableWithInitializer(
      FilterAssociationRuleImpliedCinds.ASSOCIATION_RULE_BROADCAST,
      FilterAssociationRuleImpliedCinds.AssocationRuleBroadcastInitializer)
  }

  override def filter(cind: Cind): Boolean = {
    // Copy all relevant values (all others are set already).
    this.testCind.depCaptureType = cind.depCaptureType
    this.testCind.depConditionValue1 = cind.depConditionValue1
    this.testCind.refCaptureType = cind.refCaptureType
    this.testCind.refConditionValue1 = cind.refConditionValue1
    !this.impliedCinds(this.testCind)
  }
}

object FilterAssociationRuleImpliedCinds {

  val ASSOCIATION_RULE_BROADCAST = "association-rules"

  object AssocationRuleBroadcastInitializer extends BroadcastVariableInitializer[AssociationRule, Set[Cind]] {


    override def initializeBroadcastVariable(associationRules: lang.Iterable[AssociationRule]): Set[Cind] = {
      associationRules.map { associationRule =>
        val conditionCode = associationRule.antecedentType | associationRule.consequentType
        val captureCode = ConditionCodes.addSecondaryConditions(conditionCode)
        val projectionCode = captureCode & ~conditionCode
        val cind = Cind(associationRule.antecedentType | projectionCode, associationRule.antecedent, "",
          associationRule.consequentType | projectionCode, associationRule.consequent, "")
        cind
      }.toSet
    }

  }
}