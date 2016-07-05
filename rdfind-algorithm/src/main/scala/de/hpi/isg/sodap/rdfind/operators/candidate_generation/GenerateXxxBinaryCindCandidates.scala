package de.hpi.isg.sodap.rdfind.operators.candidate_generation

import java.lang.Iterable

import de.hpi.isg.sodap.rdfind.data.Cind
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer


/**
 * Merges CINDs with the same dependent capture to new CIND candidates by merging their referenced captures if
 * possible.
 *
 * Created by basti on 5/20/15.
 */
abstract class GenerateXxxBinaryCindCandidates extends GroupReduceFunction[Cind, Cind] {
  
  lazy val output: Cind = Cind(0, null, null, 0, null, null)

  lazy val buffer = ArrayBuffer[Cind]()

  override def reduce(knownDependencies: Iterable[Cind], out: Collector[Cind]): Unit = {
    this.buffer.clear()
    this.buffer ++= knownDependencies

    // Evaluate all (sorted) pairs of 1/1 INDs with the same left-hand side.
    if (this.buffer.length > 1) {
      val sortedCinds = this.buffer.sortBy(_.refCaptureType)
      if (sortedCinds.size != this.buffer.size) {
        throw new RuntimeException
      }
      for (i <- 0 until (sortedCinds.size - 1);
           j <- i + 1 until sortedCinds.size) {
        val overlap1 = sortedCinds(i)
        val overlap2 = sortedCinds(j)

        // Figure out if the overlaps yield a valid candidate:
        // a) Their reference must address the same target field.
        if (extractSecondaryConditions(overlap1.refCaptureType) == extractSecondaryConditions(overlap2.refCaptureType)) {
          // b) Their reference must use different condition fields.
          if (extractPrimaryConditions(overlap1.refCaptureType) != extractPrimaryConditions(overlap2.refCaptureType)) {
            // c) The emerging condition must be frequent.
            // TODO or handle it later...?

            // Create the candidate.
            output.depCaptureType = overlap1.depCaptureType
            output.depConditionValue1 = overlap1.depConditionValue1
            output.depConditionValue2 = overlap1.depConditionValue2
            output.refCaptureType = overlap1.refCaptureType | overlap2.refCaptureType
            // Now the order of the overlaps/INDs is relevant!!!
            output.refConditionValue1 = overlap1.refConditionValue1
            output.refConditionValue2 = overlap2.refConditionValue1
            out.collect(output)
          }
        }
      }
    }

    // Create CIND candidates that incorporate trivial CINDs in the generation.
    generateRefinedCinds(this.buffer, this.output, out)
  }

  def generateRefinedCinds(knownCinds : IndexedSeq[Cind], output: Cind, out: Collector[Cind]): Unit
}

