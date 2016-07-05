package de.hpi.isg.sodap.rdfind.operators.frequent_conditions

import de.hpi.isg.sodap.flink.util.GlobalIdGenerator
import de.hpi.isg.sodap.rdfind.data.{RDFTriple, UnaryConditionEvidence}
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
 * This function creates [[UnaryConditionEvidence]]s for each input [[RDFTriple]]. To this end, it creates a unique
 * ID for each triple.
 *
 * Created by basti on 8/17/15.
 */
class CreateUnaryConditionEvidences extends RichFlatMapFunction[RDFTriple, UnaryConditionEvidence] {

  val idGenerator: GlobalIdGenerator = new GlobalIdGenerator(0)

  lazy val output = UnaryConditionEvidence(0, null, 1, new Array[Long](1))

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    this.idGenerator.initialize(getRuntimeContext)
  }

  override def flatMap(triple: RDFTriple, out: Collector[UnaryConditionEvidence]): Unit = {
    val tripleId = this.idGenerator.yieldLong()
    this.output.tripleIds(0) = tripleId
    this.output.conditionType = subjectCondition
    this.output.value = triple.subj
    out.collect(output)

    output.conditionType = predicateCondition
    output.value = triple.pred
    out.collect(output)

    output.conditionType = objectCondition
    output.value = triple.obj
    out.collect(output)
  }
}
