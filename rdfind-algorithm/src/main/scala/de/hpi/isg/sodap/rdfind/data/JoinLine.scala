package de.hpi.isg.sodap.rdfind.data

/**
 * @author sebastian.kruse 
 * @since 15.04.2015
 */
case class JoinLine(var conditions: Array[Condition],
                    var numUnaryConditions: Int = -1,
                    var numBinaryConditions: Int = -1,
                    var partitionKey: Int = -1,
                    var numPartitions: Int = -1) {

  def numCombinedConditions =
    if (numUnaryConditions == -1 || numBinaryConditions == -1)
      -1
    else
      numUnaryConditions + numBinaryConditions

}
