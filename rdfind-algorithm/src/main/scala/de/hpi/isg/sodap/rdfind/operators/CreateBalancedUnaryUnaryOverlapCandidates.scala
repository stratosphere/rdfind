package de.hpi.isg.sodap.rdfind.operators

import de.hpi.isg.sodap.rdfind.data._
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
class CreateBalancedUnaryUnaryOverlapCandidates(isUseAssociationRules: Boolean)
  extends AbstractExtractBalancedUnaryUnaryOverlapCandidates[OverlapSet](isUseAssociationRules) {

  private lazy val output = OverlapSet(0, null, null, 0, null)

  // We do not need this function as we override its more specific sibling.
  override def output(lhsConditionCount: ConditionCount, rhsConditionCounts: ArrayBuffer[ConditionCount],
                      out: Collector[OverlapSet]): Unit = {
    this.output.update(depConditionCount = lhsConditionCount)
    this.output.rhsConditions = rhsConditionCounts.toArray
    out.collect(this.output)
  }
}

