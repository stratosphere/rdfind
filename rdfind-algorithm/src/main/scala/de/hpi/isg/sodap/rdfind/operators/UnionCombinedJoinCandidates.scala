package de.hpi.isg.sodap.rdfind.operators

import java.lang

import de.hpi.isg.sodap.rdfind.data.{CombinedJoinCandidates, Condition, JoinLine}
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * This function takes a set of [[de.hpi.isg.sodap.rdfind.data.JoinCandidate]]s and produces a [[de.hpi.isg.sodap.rdfind.data.JoinLine]] by concatenating the candidates' conditions.
 *
 * @author sebastian.kruse 
 * @since 08.04.2015
 */
class UnionCombinedJoinCandidates extends GroupReduceFunction[CombinedJoinCandidates, JoinLine] {

  lazy val window = mutable.SortedSet[Condition]()

  override def reduce(joinCandidates: lang.Iterable[CombinedJoinCandidates], out: Collector[JoinLine]): Unit = {
    val joinCandidateIterator = joinCandidates.iterator()

    // Process windows until we are done.
    while (joinCandidateIterator.hasNext) {
      val joinCandidates = joinCandidateIterator.next()
      window ++= joinCandidates.conditions
    }
    out.collect(JoinLine(window.toArray))
    this.window.clear()
  }
}
