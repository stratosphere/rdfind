package de.hpi.isg.sodap.rdfind.operators

import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.util.BloomFilterParameters
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

/**
 * This strategy tries to distribute all overlap candidates equally over all candidate sets. For a
 * set with n unary captures, we expect approximately n/2 RHS captures for each LHS capture. We achieve this by
 * defining a non-transitive, directed relation on the hashes of the captures.
 *
 * @author sebastian.kruse
 * @since 08.05.2015
 */
class ExtractBalancedHalfApproximateUnaryUnaryOverlapCandidates(explicitThreshold: Int,
                                                                bloomFilterParameters: BloomFilterParameters[ConditionCount],
                                                                isUseAssociationRules: Boolean)
  extends AbstractExtractBalancedUnaryUnaryOverlapCandidates[HalfApproximateOverlapSet](isUseAssociationRules) {

  private lazy val output = HalfApproximateOverlapSet(0, null, null, 0, null, null)

  private lazy val fuzzyRhsConditionCounts = bloomFilterParameters.createSpectralBloomFilter

  // We do not need this function as we override its more specific sibling.
  override def output(lhsConditionCount: ConditionCount, rhsConditionCounts: ArrayBuffer[ConditionCount],
                      out: Collector[HalfApproximateOverlapSet]): Unit = {

    this.output.update(depConditionCount = lhsConditionCount)
    if (rhsConditionCounts.size > explicitThreshold) {
      rhsConditionCounts.foreach { conditionCount =>
        this.fuzzyRhsConditionCounts.putToSetBatch(conditionCount)
      }
      this.fuzzyRhsConditionCounts.executeSetBatch()
      this.output.rhsConditions = Array()
      this.output.approximateRhsConditions = this.fuzzyRhsConditionCounts.exportBits()
    } else {
      this.output.rhsConditions = rhsConditionCounts.toArray
      this.output.approximateRhsConditions = Array()
    }
    out.collect(this.output)
    this.fuzzyRhsConditionCounts.clear()
  }
}

