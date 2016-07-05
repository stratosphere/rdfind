package de.hpi.isg.sodap.rdfind.data

import de.hpi.isg.sodap.rdfind.util.ConditionCodes

/**
 * @author sebastian.kruse 
 * @since 15.04.2015
 */
case class UnaryConditionEvidence(var conditionType: Int, var value: String, var count: Int,
                                   var tripleIds : Array[Long])

