package de.hpi.isg.sodap.rdfind.operators.frequent_conditions

import java.lang.Iterable
import java.util

import de.hpi.isg.sodap.rdfind.data.UnaryConditionEvidence
import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.api.common.functions.RichGroupReduceFunction.Combinable
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

/**
 * This function merges [[UnaryConditionEvidence]]s with the same unary condition. The counts are summed up
 * and the triple IDs are concatenated.
 *
 * Created by basti on 8/17/15.
 */
@Combinable
class MergeUnaryConditionEvidences extends RichGroupReduceFunction[UnaryConditionEvidence, UnaryConditionEvidence] {

  val tripleIdCollector = ArrayBuffer[Long]()

  val outputEvidence = UnaryConditionEvidence(0, null, 0, null)

  override def reduce(in: Iterable[UnaryConditionEvidence], out: Collector[UnaryConditionEvidence]): Unit = {
    val inIterator: util.Iterator[UnaryConditionEvidence] = in.iterator()
    val firstEvidence: UnaryConditionEvidence = inIterator.next()
    if (!inIterator.hasNext) {
      out.collect(firstEvidence)
      return
    }
    this.outputEvidence.conditionType = firstEvidence.conditionType
    this.outputEvidence.value = firstEvidence.value

    this.tripleIdCollector.clear()
    this.tripleIdCollector ++= firstEvidence.tripleIds
    var count = firstEvidence.count

    while (inIterator.hasNext) {
      val nextEvidence = inIterator.next()
      this.tripleIdCollector ++= nextEvidence.tripleIds
      count += nextEvidence.count
    }
    this.outputEvidence.count = count
    this.outputEvidence.tripleIds = this.tripleIdCollector.toArray

    out.collect(this.outputEvidence)
  }

}
