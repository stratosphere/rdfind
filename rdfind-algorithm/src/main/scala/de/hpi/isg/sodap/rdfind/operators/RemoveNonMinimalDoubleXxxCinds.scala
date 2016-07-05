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
class RemoveNonMinimalDoubleXxxCinds extends CoGroupFunction[Cind, Cind, Cind] {

  override def coGroup(impliedCinds: lang.Iterable[Cind], implyingCinds: lang.Iterable[Cind],
                       out: Collector[Cind]): Unit = {

    // Build a probing table of dependent captures of the right CINDs.
    val probingTable = new mutable.HashMap[Int, mutable.Set[String]]
    implyingCinds.foreach { implyingCind =>
      val captureValues = probingTable.getOrElseUpdate(implyingCind.depCaptureType, new mutable.HashSet[String])
      captureValues += implyingCind.depConditionValue1
    }

    // Probe the potentially implied CINDs and collect only when probing did not succeed.
    impliedCinds.foreach { impliedCind =>
      val isImplied = (probingTable.get(ConditionCodes.extractFirstSubcapture(impliedCind.depCaptureType)) match {
        case Some(values) => values.contains(impliedCind.depConditionValue1)
        case None => false
      }) || (probingTable.get(ConditionCodes.extractSecondSubcapture(impliedCind.depCaptureType)) match {
        case Some(values) => values.contains(impliedCind.depConditionValue2)
        case None => false
      })
      if (!isImplied) out.collect(impliedCind)
    }

  }
}
