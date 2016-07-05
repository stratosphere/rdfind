package de.hpi.isg.sodap.rdfind.operators

import com.google.common.hash.BloomFilter
import com.google.common.hash.data.BitArray
import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.operators.CreateReducedBalancedUnaryUnaryOverlapCandidates._
import de.hpi.isg.sodap.rdfind.util.BloomFilterParameters
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * This strategy tries to distribute all overlap candidates equally over all candidate sets. For a
 * set with n unary captures, we expect approximately n/2 RHS captures for each LHS capture. We achieve this by
 * defining a non-transitive, directed relation on the hashes of the captures.
 *
 * @author sebastian.kruse
 * @since 08.05.2015
 */
class CreateReducedBalancedUnaryUnaryOverlapCandidates(bloomFilterParameters: BloomFilterParameters[ConditionCount] = null,
                                                       explicitThreshold: Int = -1,
                                                       isUseAssociationRules: Boolean)
  extends AbstractExtractBalancedUnaryUnaryOverlapCandidates[OverlapSet](isUseAssociationRules) {

  private lazy val output = OverlapSet(0, null, null, 0, null)

  private var explicitApproximateCandidates: mutable.Map[Condition, Set[Condition]] = _

  private var curExplicitApproximateCandidates: Set[Condition] = _

  private var fuzzyApproximateCandidates: mutable.Map[Condition, BloomFilter[ConditionCount]] = _

  private var curFuzzyApproximateCandidates: BloomFilter[ConditionCount] = _

  private lazy val testCondition = Condition(null, null, 0)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val broadcast = getRuntimeContext.getBroadcastVariable[HalfApproximateOverlapSet](APPROXIMATE_OVERLAPS_BROADCAST)

    // We need to find out, how many bits the Bloom filters have, that are derived from a spectral Bloom filter.
    // This is equal to finding out the size of a spectral Bloom filter.
    val bloomFilterSize = bloomFilterParameters.createSpectralBloomFilter.size()

    this.explicitApproximateCandidates = new mutable.HashMap
    this.fuzzyApproximateCandidates = new mutable.HashMap
    broadcast.foreach { halfApproximateOverlapSet =>
      if (halfApproximateOverlapSet.rhsConditions.length > 0) {
        this.explicitApproximateCandidates.put(halfApproximateOverlapSet.depCondition.decoalesce,
          halfApproximateOverlapSet.rhsConditions.map(_.toCondition.decoalesce()).toSet)
      }
      if (halfApproximateOverlapSet.approximateRhsConditions.length > 0) {
        val bits = new BitArray(halfApproximateOverlapSet.approximateRhsConditions, bloomFilterSize)
        val bloomFilter = bloomFilterParameters.createBloomFilter(bits)
        this.fuzzyApproximateCandidates.put(halfApproximateOverlapSet.depCondition.decoalesce(), bloomFilter)
      }
    }
  }


  /**
   * Helper function for load balancing: tells whether this dependent condition should be processed for this join
   * line split.
   */
  override def shouldProcess[X](joinLine: JoinLine, dependentCondition: X): Boolean =
    super.shouldProcess(joinLine, dependentCondition) && {
      val condition = dependentCondition.asInstanceOf[ConditionCount].toCondition(this.testCondition)
      this.curExplicitApproximateCandidates = this.explicitApproximateCandidates.getOrElse(condition, null)
      this.curFuzzyApproximateCandidates = this.fuzzyApproximateCandidates.getOrElse(condition, null)
      this.curExplicitApproximateCandidates != null || this.curFuzzyApproximateCandidates != null
    }


  /** Tells whether to consider the given condition count as RHS for the LHS condition count. */
  override def shouldAdd(rhsConditionCount: ConditionCount): Boolean = {
    super.shouldAdd(rhsConditionCount) && {
      (this.curExplicitApproximateCandidates != null && {
        val condition = rhsConditionCount.toCondition(this.testCondition)
        this.curExplicitApproximateCandidates.contains(condition)
      }) || (this.curFuzzyApproximateCandidates != null &&
        this.curFuzzyApproximateCandidates.mightContain(rhsConditionCount))
    }
  }

  // We do not need this function as we override its more specific sibling.
  override def output(lhsConditionCount: ConditionCount, rhsConditionCounts: ArrayBuffer[ConditionCount],
                      out: Collector[OverlapSet]): Unit = {
    this.output.update(depConditionCount = lhsConditionCount)
    this.output.rhsConditions = rhsConditionCounts.toArray
    out.collect(this.output)
  }
}

object CreateReducedBalancedUnaryUnaryOverlapCandidates {
  val APPROXIMATE_OVERLAPS_BROADCAST = "approximate-overlaps"
}
