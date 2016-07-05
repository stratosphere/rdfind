package de.hpi.isg.sodap.rdfind.operators.candidate_generation

import de.hpi.isg.sodap.rdfind.data.Cind
import de.hpi.isg.sodap.rdfind.util.ConditionCodes
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
 * @author Sebastian
 * @since 26.05.2015.
 */
class GenerateBinaryBinaryCindCandidates
extends GenerateXxxBinaryCindCandidates {


  override def generateRefinedCinds(knownCinds: IndexedSeq[Cind], output: Cind, out: Collector[Cind]): Unit = { 
    // Consider trivial double-single CINDs that were not considered above.
    // For instance, s[p=p1,o=o1] < s[o=o1] holds naturally
    // Given s[p=p1, o=o1] < s[p=p2],
    // we would also need to consider s[p=p1,o=o1] < s[p=p2,o=o1].
    this.buffer.foreach { doubleSingleCind =>
      if (ConditionCodes.isSubcode(doubleSingleCind.refCaptureType, doubleSingleCind.depCaptureType)) {
        output.depConditionValue1 = doubleSingleCind.depConditionValue1
        output.depConditionValue2 = doubleSingleCind.depConditionValue2
        output.depCaptureType = doubleSingleCind.depCaptureType
        output.refCaptureType = doubleSingleCind.depCaptureType

        if (ConditionCodes.extractFirstSubcapture(doubleSingleCind.depCaptureType) == doubleSingleCind.refCaptureType) {
          output.refConditionValue1 = doubleSingleCind.refConditionValue1
          output.refConditionValue2 = doubleSingleCind.depConditionValue2
        } else {
          output.refConditionValue1 = doubleSingleCind.depConditionValue1
          output.refConditionValue2 = doubleSingleCind.refConditionValue1
        }
        out.collect(output)
      }
    }
  }

}
