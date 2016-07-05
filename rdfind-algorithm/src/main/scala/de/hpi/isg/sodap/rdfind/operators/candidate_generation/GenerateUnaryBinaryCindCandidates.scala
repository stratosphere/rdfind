package de.hpi.isg.sodap.rdfind.operators.candidate_generation

import de.hpi.isg.sodap.rdfind.data.Cind
import de.hpi.isg.sodap.rdfind.util.ConditionCodes
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import org.apache.flink.util.Collector

/**
 * @author Sebastian
 * @since 26.05.2015.
 */
class GenerateUnaryBinaryCindCandidates
extends GenerateXxxBinaryCindCandidates {


  override def generateRefinedCinds(knownCinds: IndexedSeq[Cind], output: Cind, out: Collector[Cind]): Unit = {
    // Consider trivial single-single CINDs that were not considered above.
    // E.g., if we have s[p1] < s[o1], then we need to consider that there implicitly is s[p1] < s[p1].
    // Then we also need to create s[p1] < s[p1,o1], which is not trivial.
    this.buffer.foreach { singleSingleCind =>
      val primaryDepCondition = extractPrimaryConditions(singleSingleCind.depCaptureType)
      val primaryRefCondition = extractPrimaryConditions(singleSingleCind.refCaptureType)
      if ((primaryDepCondition != primaryRefCondition) &&
        (extractSecondaryConditions(singleSingleCind.depCaptureType) ==
          extractSecondaryConditions(singleSingleCind.refCaptureType))) {
        output.depCaptureType = singleSingleCind.depCaptureType
        output.depConditionValue1 = singleSingleCind.depConditionValue1
        output.depConditionValue2 = singleSingleCind.depConditionValue2
        output.refCaptureType = merge(singleSingleCind.refCaptureType, singleSingleCind.depCaptureType)
        if (primaryDepCondition < primaryRefCondition) {
          // e.g., s[p=...] < s[o=...] -> s[p=...,o=...]
          output.refConditionValue1 = singleSingleCind.depConditionValue1
          output.refConditionValue2 = singleSingleCind.refConditionValue1
        } else {
          output.refConditionValue1 = singleSingleCind.refConditionValue1
          output.refConditionValue2 = singleSingleCind.depConditionValue1
        }
        out.collect(output)
      }
    }
  }

}
