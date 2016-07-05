package de.hpi.isg.sodap.rdfind.operators.candidate_generation

import java.lang.Iterable

import de.hpi.isg.sodap.rdfind.data.Cind
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._
import scala.util.Sorting
import scala.collection.mutable.ArrayBuffer

/**
 * @author Sebastian
 * @since 26.05.2015.
 */
class GenerateBinaryUnaryCindCandidates extends GroupReduceFunction[Cind, Cind] {

  lazy val output = Cind(0, null, null, 0, null, null)

  lazy val buffer = ArrayBuffer[Cind]()

  override def reduce(iterable: Iterable[Cind], out: Collector[Cind]): Unit = {

    // Evaluate all (sorted) pairs of proper 1/1 overlaps with the same right-hand side.
    this.buffer.clear()
    this.buffer ++= iterable

    // The sorting of the pairs ensures that we merge captures for the CIND candidates correctly.
    if (this.buffer.size > 1) {
      val sortedBuffer = this.buffer.sortBy(_.depCaptureType)
      for (i <- 0 until (sortedBuffer.length - 1);
           j <- i + 1 until sortedBuffer.length) {
        val overlap1 = sortedBuffer(i)
        val overlap2 = sortedBuffer(j)

        // Figure out if the overlaps yield a valid candidate:
        // a) Their left-hand side must address the same target field.
        // b) Their reference must use different condition fields.
        if (extractSecondaryConditions(overlap1.depCaptureType) == extractSecondaryConditions(overlap2.depCaptureType)
          && extractPrimaryConditions(overlap1.depCaptureType) != extractPrimaryConditions(overlap2.depCaptureType)) {
          // TODO: At this point, we could also incorporate a frequency test of the created binary condition.
          //       It *might* reduce the candidate cardinality and therefore the Bloom filter accuracy.

          // Create the candidate.
          // Now the order of the overlaps/INDs is relevant!!!
          output.depCaptureType = overlap1.depCaptureType | overlap2.depCaptureType
          output.depConditionValue1 = overlap1.depConditionValue1
          output.depConditionValue2 = overlap2.depConditionValue1
          output.refCaptureType = overlap1.refCaptureType
          output.refConditionValue1 = overlap1.refConditionValue1
          output.refConditionValue2 = overlap1.refConditionValue2
          out.collect(output)
        }
      }
    }
  }
}
