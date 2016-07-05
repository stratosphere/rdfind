package de.hpi.isg.sodap.rdfind.operators

import java.lang.Iterable

import de.hpi.isg.sodap.rdfind.data.Cind
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * This function infers non-minimal 2/1-CINDs from 1/1-CINDs and -DVOs. This input should be grouped by the
 * right-hand side capture condition and value.
 *
 * @author sebastian.kruse 
 * @since 20.04.2015
 */
class InferDoubleSingleCinds extends GroupReduceFunction[Cind, Cind] {

  lazy val buffer = new ArrayBuffer[Cind]

  lazy val outputCandidate = Cind(0, null, null, 0, null, null)

  override def reduce(dependencies: Iterable[Cind], out: Collector[Cind]): Unit = {
    this.buffer.clear()
    this.buffer ++= dependencies

    if (this.buffer.size < 2) return

    val sortedBuffer = this.buffer.sortBy(_.depCaptureType)
    // Iterate over all pairs and create 2/1 CINDs.
    for (i <- 0 until sortedBuffer.size - 1; dependency1 = sortedBuffer(i);
         j <- i + 1 until sortedBuffer.size; dependency2 = sortedBuffer(j)) {

      // Check if the lhs captures can be merged.
      val isDependency1Cind = dependency1.support == 0
      val isDependency2Cind = dependency2.support == 0
      if ((isDependency1Cind || isDependency2Cind) &&
        extractSecondaryConditions(dependency1.depCaptureType) == extractSecondaryConditions(dependency2.depCaptureType) &&
        (extractPrimaryConditions(dependency1.depCaptureType) & extractPrimaryConditions(dependency2.depCaptureType)) == 0) {

        // Create the 2/1-CIND candidate (respect order of dependencies!)
        this.outputCandidate.depCaptureType = dependency1.depCaptureType | dependency2.depCaptureType
        this.outputCandidate.depConditionValue1 = dependency1.depConditionValue1
        this.outputCandidate.depConditionValue2 = dependency2.depConditionValue1
        this.outputCandidate.refCaptureType = dependency1.refCaptureType
        this.outputCandidate.refConditionValue1 = dependency1.refConditionValue1
        this.outputCandidate.refConditionValue2 = ""
        out.collect(this.outputCandidate)
      }
    }
  }
}
