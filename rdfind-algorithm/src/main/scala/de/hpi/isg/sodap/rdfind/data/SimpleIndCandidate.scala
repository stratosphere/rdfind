package de.hpi.isg.sodap.rdfind.data


/**
 * @author sebastian.kruse 
 * @since 15.04.2015
 */
case class SimpleIndCandidate(var lhsConditionType: Int,
                              var lhsCondition1: String,
                              var lhsCondition2: String,
                              var rhsConditions: Array[Condition])
