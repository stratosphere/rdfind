package de.hpi.isg.sodap.rdfind.data

import com.google.common.hash.BloomFilter

/**
 * @author sebastian.kruse
 */
case class ApproximateCindSet(var depConditionType: Int,
                              var depConditionValue1: String,
                              var depConditionValue2: String,
                              var depCount: Int,
                              var refConditions: BloomFilter[Condition])
