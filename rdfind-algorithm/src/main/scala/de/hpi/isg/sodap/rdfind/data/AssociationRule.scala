package de.hpi.isg.sodap.rdfind.data

import de.hpi.isg.sodap.rdfind.util.ConditionCodes

/**
 * @author sebastian.kruse 
 * @since 15.04.2015
 */
case class AssociationRule(var antecedentType: Int,
                           var consequentType: Int,
                           var antecedent: String,
                           var consequent: String,
                           var support: Int,
                           var confidence: Double) {
  override def toString: String = s"${ConditionCodes.prettyPrint(antecedentType, antecedent)} -> " +
    s"${ConditionCodes.prettyPrint(consequentType, consequent)} " +
    s"(support=$support," +
    s"confidence=${"%3.2f".format(confidence * 100d)}%)"

}
