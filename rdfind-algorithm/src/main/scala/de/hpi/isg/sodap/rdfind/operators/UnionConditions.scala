package de.hpi.isg.sodap.rdfind.operators

import de.hpi.isg.sodap.rdfind.data.{Condition, JoinCandidate, JoinLine}
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * This function takes a set of [[de.hpi.isg.sodap.rdfind.data.JoinCandidate]]s and produces a [[de.hpi.isg.sodap.rdfind.data.JoinLine]] by concatenating the candidates' conditions.
 *
 * @author sebastian.kruse 
 * @since 08.04.2015
 */
class UnionConditions extends GroupReduceFunction[JoinCandidate, JoinLine] {

  def reduce(joinCandidates: java.lang.Iterable[JoinCandidate], out: Collector[JoinLine]): Unit = {
    val conditions = mutable.HashSet[Condition]()
    var joinValue: String = null
    joinCandidates.foreach {
      joinCandidate =>
        conditions += Condition(joinCandidate.conditionValue1, joinCandidate.conditionValue2, joinCandidate.conditionType)
        joinValue = joinCandidate.joinValue
    }

    out.collect(JoinLine(conditions.toArray))

    // println(s"${conditions.size} conditions for $joinValue: $conditions")

    // TODO: We could store a sketch of singleton conditions -> no INDs.
  }

}
