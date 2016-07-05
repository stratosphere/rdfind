package de.hpi.isg.sodap.rdfind.operators

import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.operators.CreateBinaryUnaryCindCandidates._
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import org.apache.flink.configuration.Configuration
import com.google.common.hash.BloomFilter
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * @author sebastian.kruse
 * @since 08.05.2015
 */
class CreateBinaryUnaryCindCandidates(isTestBinaryCaptureFrequency: Boolean = false)
  extends CreateDependencyCandidates[CindSet, Condition, Condition](true, true, false) {

  var frequentDoubleConditionsFilters: Map[Int, BloomFilter[Condition]] = _

  var candidateFilter: BloomFilter[Cind] = _

  lazy val output = CindSet(0, null, null, 0, null)

  lazy val candidateFilterCind = Cind(0, null, null, 0, null, null)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // Load the Bloom filter for the CIND candidates.
    val candidateBloomFilters = this.getRuntimeContext.getBroadcastVariable[BloomFilter[Cind]](
      CANDIDATE_BLOOM_FILTER_BROADCAST)
    if (candidateBloomFilters.size() != 1) {
      throw new IllegalStateException(s"Expected one candidate Bloom filter, found ${candidateBloomFilters.size()}.")
    }
    this.candidateFilter = candidateBloomFilters.get(0)

    // Load the Bloom filters for the frequent double conditions.
    if (isTestBinaryCaptureFrequency) {
      val conditionBloomFilters = this.getRuntimeContext.getBroadcastVariable[(Int, BloomFilter[Condition])](
        FREQUENT_BINARY_CAPTURES_BROADCAST)
      this.frequentDoubleConditionsFilters = conditionBloomFilters.toMap
    }
  }

  override protected def createUnaryConditions: mutable.Set[Condition] = mutable.SortedSet()

  override protected def createBinaryConditions: mutable.Set[Condition] = mutable.Set()

  override def collectBinaryCaptures(collector: mutable.Set[Condition], condition: Condition): Unit =
    if (!this.isTestBinaryCaptureFrequency || {
      val filter = this.frequentDoubleConditionsFilters(condition.conditionType)
      filter.mightContain(condition)
    }) {
      collector += condition
    }

  override protected def collectUnaryCapture(collector: mutable.Set[Condition], condition: Condition): Unit =
    collector += condition

  override def splitAndCollectUnaryCaptures(collector: mutable.Set[Condition], condition: Condition): Unit = {
    val conditions = decodeConditionCode(condition.conditionType, isRequireDoubleCode = true)
    val newConditionCode1 = createConditionCode(conditions._1, secondaryCondition = conditions._3)
    collector += Condition(condition.conditionValue1, null, newConditionCode1)
    val newConditionCode2 = createConditionCode(conditions._2, secondaryCondition = conditions._3)
    collector += Condition(condition.conditionValue2, null, newConditionCode2)
  }

  override def collectDependencyCandidates(unaryConditions: mutable.Set[Condition],
                                           binaryConditions: mutable.Set[Condition],
                                           out: Collector[CindSet]): Unit = {
    binaryConditions.foreach { binaryCapture =>
      this.candidateFilterCind.update(depCapture = binaryCapture)
      val refConditions = unaryConditions.filter { unaryCapture =>
        !binaryCapture.implies(unaryCapture) && {
          this.candidateFilterCind.update(refCapture = unaryCapture)
          this.candidateFilter.mightContain(this.candidateFilterCind)
        }
      }.toArray

      this.output.update(depCondition = binaryCapture)
      this.output.depCount = 1
      this.output.refConditions = refConditions
      out.collect(this.output)
    }
  }
}

object CreateBinaryUnaryCindCandidates {
  val FREQUENT_BINARY_CAPTURES_BROADCAST: String = "frequent-two-captures-broadcast"
  val CANDIDATE_BLOOM_FILTER_BROADCAST: String = "candidates-broadcast"
}
