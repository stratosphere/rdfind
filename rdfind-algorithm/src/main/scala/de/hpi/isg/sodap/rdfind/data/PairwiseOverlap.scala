package de.hpi.isg.sodap.rdfind.data

/**
 * This class represents the overlap of two captures whose distinct value counts are known.
 *
 * @author sebastian.kruse 
 * @since 08.05.2015
 */
case class PairwiseOverlap(smallConditionCount: ConditionCount, largeConditionCount: ConditionCount, overlap: Int)
