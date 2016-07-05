package de.hpi.isg.sodap.rdfind.operators

import java.lang

import de.hpi.isg.sodap.rdfind.data.Cind
import de.hpi.isg.sodap.rdfind.util.ConditionCodes
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * @author sebastian.kruse 
 * @since 22.04.2015
 */
class RemoveNonMinimalXxxSingleCinds extends CoGroupFunction[Cind, Cind, Cind] {

  override def coGroup(impliedCinds: lang.Iterable[Cind], implyingCinds: lang.Iterable[Cind],
                       out: Collector[Cind]): Unit = {

    // Build a probing table of referenced captures of the right CINDs.
    val probingTable = new mutable.HashMap[Int, mutable.Set[String]]
    implyingCinds.foreach { implyingCind =>
      val firstSubcaptureCode = ConditionCodes.extractFirstSubcapture(implyingCind.refCaptureType)
      val firstSubcaptureValues = probingTable.getOrElseUpdate(firstSubcaptureCode, new mutable.HashSet[String])
      firstSubcaptureValues += implyingCind.refConditionValue1

      val secondSubcaptureCode = ConditionCodes.extractSecondSubcapture(implyingCind.refCaptureType)
      val secondSubcaptureValues = probingTable.getOrElseUpdate(secondSubcaptureCode, new mutable.HashSet[String])
      secondSubcaptureValues += implyingCind.refConditionValue2
    }

    // Probe the potentially implied CINDs and collect only when probing did not succeed.
    impliedCinds.foreach { impliedCind =>
      probingTable.get(impliedCind.refCaptureType) match {
        case Some(values) => if (!values.contains(impliedCind.refConditionValue1)) out.collect(impliedCind)
        case None => out.collect(impliedCind)
      }
    }

  }
}
