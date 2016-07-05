package de.hpi.isg.sodap.rdfind.operators

import de.hpi.isg.sodap.rdfind.data.{JoinLine, JoinLineLoad}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.functions.FunctionAnnotation.ReadFields

/**
 * @author sebastian.kruse 
 * @since 09.06.2015
 */
@ReadFields(Array("numUnaryConditions", "numBinaryConditions"))
class DetermineJoinLineLoads extends RichMapFunction[JoinLine, JoinLineLoad]{

  override def map(joinLine: JoinLine): JoinLineLoad = JoinLineLoad(joinLine.numUnaryConditions, joinLine.numBinaryConditions)

}
