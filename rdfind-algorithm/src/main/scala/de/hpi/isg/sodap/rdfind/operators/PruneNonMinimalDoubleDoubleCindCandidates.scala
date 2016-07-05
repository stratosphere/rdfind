package de.hpi.isg.sodap.rdfind.operators

import java.lang

import de.hpi.isg.sodap.rdfind.data.Cind
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.util.Collector

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer


/**
 * This function removes 2/2 CIND candidates that are implied by a set of given 1/2 CINDs.
 * 
 * @author sebastian.kruse 
 * @since 20.04.2015
 */
class PruneNonMinimalDoubleDoubleCindCandidates extends CoGroupFunction[Cind, Cind,
  Cind] {

  lazy val singleDoubleCindBuffer = new ArrayBuffer[Cind]

  override def coGroup(candidates: lang.Iterable[Cind], unaryBinaryCinds: lang.Iterable[Cind],
                       out: Collector[Cind]): Unit = {

    // Materialize the 2/1-CINDs.
    this.singleDoubleCindBuffer.clear()
    this.singleDoubleCindBuffer ++= unaryBinaryCinds

    // Shortcut: If there is no 2/1-CIND at all, the 2/2 candidates are minimal.
    if (this.singleDoubleCindBuffer.isEmpty) {
      candidates.foreach(out.collect(_))
      return
    }

    // Check each candidate for minimality.
    candidates.foreach { candidate =>

      @tailrec
      def checkMinimality(singleDoubleCindIndex: Int): Boolean = {
        val singleDoubleCind = this.singleDoubleCindBuffer(singleDoubleCindIndex)

        var isMinimal = true

        // Find out if the conditions match.
        if (isSubcode(singleDoubleCind.depCaptureType, candidate.depCaptureType)) {

          // Find out if the first or second value is potentially included and compare.
          val decodedCondition = decodeConditionCode(candidate.depCaptureType)
          if (isSubcode(decodedCondition._1, singleDoubleCind.depCaptureType)) {
            isMinimal &= candidate.depConditionValue1 != singleDoubleCind.depConditionValue1
          } else {
            isMinimal &= candidate.depConditionValue2 != singleDoubleCind.depConditionValue1
          }
        }

        if (!isMinimal && this.singleDoubleCindBuffer.size < singleDoubleCindIndex + 1) {
          checkMinimality(singleDoubleCindIndex + 1)
        } else isMinimal
      }

      if (checkMinimality(0)) {
        out.collect(candidate)
      }

    }
  }
}
