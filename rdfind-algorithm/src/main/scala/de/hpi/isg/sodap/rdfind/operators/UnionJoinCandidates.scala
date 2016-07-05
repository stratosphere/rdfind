package de.hpi.isg.sodap.rdfind.operators

import java.lang

import de.hpi.isg.sodap.rdfind.data.{CombinedJoinCandidates, Condition, JoinCandidate, JoinLine}
import org.apache.flink.api.common.functions.{GroupCombineFunction, GroupReduceFunction}
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * This function takes a set of [[de.hpi.isg.sodap.rdfind.data.JoinCandidate]]s and produces a [[de.hpi.isg.sodap.rdfind.data.JoinLine]] by concatenating the candidates' conditions.
 *
 * @author sebastian.kruse 
 * @since 08.04.2015
 */
class UnionJoinCandidates(windowSize : Int = -1) extends GroupCombineFunction[JoinCandidate, CombinedJoinCandidates] {

  if (windowSize < 1 && windowSize != -1) {
    throw new IllegalArgumentException(s"Illegal window size of $windowSize.")
  }

  lazy val window = mutable.SortedSet[Condition]()

  override def combine(joinCandidates: lang.Iterable[JoinCandidate], out: Collector[CombinedJoinCandidates]): Unit = {
    val joinCandidateIterator = joinCandidates.iterator()

    var joinKey: String = null

    // Process windows until we are done.
    while (joinCandidateIterator.hasNext) {
      // Fill the window.
      while ((windowSize == -1 || window.size < windowSize) && joinCandidateIterator.hasNext) {
        val joinCandidate = joinCandidateIterator.next()
        if (joinKey == null) joinKey = joinCandidate.joinValue
        window += Condition(joinCandidate.conditionValue1, joinCandidate.conditionValue2, joinCandidate.conditionType)
      }

      out.collect(CombinedJoinCandidates(joinKey, window.toArray))
      this.window.clear()
    }
  }
}
