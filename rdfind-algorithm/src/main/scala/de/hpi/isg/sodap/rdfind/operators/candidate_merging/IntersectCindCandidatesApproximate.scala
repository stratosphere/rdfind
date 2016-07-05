package de.hpi.isg.sodap.rdfind.operators.candidate_merging

import java.lang.Iterable

import de.hpi.isg.sodap.rdfind.data.ApproximateCindSet
import org.apache.flink.api.common.functions.{GroupCombineFunction, GroupReduceFunction}
import org.apache.flink.util.Collector

/**
 * @author sebastian.kruse 
 * @since 05.05.2015
 */
class IntersectCindCandidatesApproximate(windowSize: Int = -1)
  extends GroupCombineFunction[ApproximateCindSet, ApproximateCindSet]
  with GroupReduceFunction[ApproximateCindSet, ApproximateCindSet] {


  override def reduce(candidates: Iterable[ApproximateCindSet], out: Collector[ApproximateCindSet]): Unit =
    combine(candidates, out)

  override def combine(candidates: Iterable[ApproximateCindSet], out: Collector[ApproximateCindSet]): Unit = {

    var accumulator : ApproximateCindSet = null
    val candidateIterator = candidates.iterator()

    // Accumulate all the candidates.
    while (candidateIterator.hasNext) {
      val nextCandidate = candidateIterator.next()
      // We cannot abort early because of the support counting.
      // Regularly aggregate the CINDs otherwise.
      if (accumulator == null) {
        accumulator = nextCandidate
      } else {
        if (accumulator.refConditions != null && nextCandidate.refConditions != null)
          accumulator.refConditions.intersect(nextCandidate.refConditions)
        else
          accumulator.refConditions = null
        accumulator.depCount += nextCandidate.depCount
      }
    }


    // Output the result.
    out.collect(accumulator)
  }
}


