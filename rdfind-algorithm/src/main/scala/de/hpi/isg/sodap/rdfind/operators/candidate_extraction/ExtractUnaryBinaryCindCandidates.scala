package de.hpi.isg.sodap.rdfind.operators.candidate_extraction

import com.google.common.hash.BloomFilter
import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.operators.CreateDependencyCandidates
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * This function creates CIND candidates with a single condition on the dependent and a double condition on the
 * referenced side from the full join of an RDF data set. Furthermore, it makes use of a Bloom filter containing
 * all interesting IND candidates and a Bloom filter containing all frequent conditions.
 *
 * @author sebastian.kruse
 * @since 08.04.2015
 */
class ExtractUnaryBinaryCindCandidates(isTestBinaryCaptureFrequency: Boolean)
  extends CreateDependencyCandidates[CindSet, Condition, Condition](true, true, false) {

  var candidateFilter: BloomFilter[Cind] = _

  lazy val candidateFilterCind = Cind(0, null, null, 0, null, null)

  var frequentDoubleConditionsFilters: Map[Int, BloomFilter[Condition]] = _

  lazy val output = CindSet(0, null, null, 0, null)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // Load the Bloom filter for the CIND candidates.
    val candidateBloomFilters = this.getRuntimeContext.getBroadcastVariable[BloomFilter[Cind]](
      ExtractUnaryBinaryCindCandidates.CANDIDATE_BLOOM_FILTER_BROADCAST)
    if (candidateBloomFilters.size() != 1) {
      throw new IllegalStateException(s"Expected one candidate Bloom filter, found ${candidateBloomFilters.size()}.")
    }
    this.candidateFilter = candidateBloomFilters.get(0)
  }

  override protected def createUnaryConditions: mutable.Set[Condition] = mutable.Set[Condition]()

  override protected def createBinaryConditions: mutable.Set[Condition] = mutable.SortedSet[Condition]()

  override protected def collectUnaryCapture(collector: mutable.Set[Condition], condition: Condition): Unit =
    collector += condition

  override def collectBinaryCaptures(collector: mutable.Set[Condition], condition: Condition): Unit =
    if (!this.isTestBinaryCaptureFrequency || {
      val filter = this.frequentDoubleConditionsFilters(condition.conditionType)
      filter.mightContain(condition)
    }) {
      collector += condition
    }

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

    unaryConditions.foreach { unaryCapture =>
      this.candidateFilterCind.update(depCapture = unaryCapture)
      val refConditions = binaryConditions.filter { binaryCapture =>
        this.candidateFilterCind.update(refCapture = binaryCapture)
        this.candidateFilter.mightContain(this.candidateFilterCind)
      }.toArray

      this.output.update(depCondition = unaryCapture)
      this.output.depCount = 1
      this.output.refConditions = refConditions
      out.collect(this.output)
    }
  }
}

object ExtractUnaryBinaryCindCandidates {
  val CANDIDATE_BLOOM_FILTER_BROADCAST = "candidate-bloom-filters"
}

