package de.hpi.isg.sodap.rdfind.operators

import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.operators.AbstractExtractBalancedUnaryUnaryOverlapCandidates._
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

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
abstract class AbstractExtractBalancedUnaryUnaryOverlapCandidates[OUT](isUseAssociationRules: Boolean)
  extends CreateDependencyCandidates[OUT, (ConditionCount, Int), Unit](true, false, isUseAssociationRules) {

  private lazy val rhsConditionCounts = new ArrayBuffer[ConditionCount]()

  private lazy val output = OverlapSet(0, null, null, 0, null)

  private lazy val testCondition = Condition(null, null, 0)


//  override def open(parameters: Configuration): Unit = {
//    super.open(parameters)
//
//    this.associationRuleImpliedCinds ++= this.associationRuleImpliedCinds.map(_.swap)
//  }

  override protected def createUnaryConditions: mutable.Set[(ConditionCount, Int)] = mutable.SortedSet()(ConditionCountWithHashOrdering)

  override def collectBinaryCaptures(collector: mutable.Set[Unit], condition: Condition): Unit = ???

  override protected def createBinaryConditions: mutable.Set[Unit] = ???

  override protected def collectUnaryCapture(collector: mutable.Set[(ConditionCount, Int)], condition: Condition): Unit = {
    val conditionCount = condition.toConditionCount(count = 1)
    conditionCount.conditionValue2 = null
    collector += Tuple2(conditionCount, conditionCount.hashCode() & HASH_BIT_MASK)
  }

  override def splitAndCollectUnaryCaptures(collector: mutable.Set[(ConditionCount, Int)], condition: Condition): Unit = {
    val conditions = decodeConditionCode(condition.conditionType, isRequireDoubleCode = true)
    val newConditionCode1 = createConditionCode(conditions._1, secondaryCondition = conditions._3)
    val conditionCount1 = ConditionCount(newConditionCode1, condition.conditionValue1, null, 1)
    collector += Tuple2(conditionCount1, conditionCount1.hashCode() & HASH_BIT_MASK)
    val newConditionCode2 = createConditionCode(conditions._2, secondaryCondition = conditions._3)
    val conditionCount2 = ConditionCount(newConditionCode2, condition.conditionValue2, null, 1)
    collector += Tuple2(conditionCount2, conditionCount2.hashCode() & HASH_BIT_MASK)
  }


  // We do not need this function as we override its more specific sibling.
  override def collectDependencyCandidates(unaryConditions: mutable.Set[(ConditionCount, Int)],
                                           binaryConditions: mutable.Set[Unit],
                                           out: Collector[OUT]): Unit = ???

  override def collectDependencyCandidates(unaryConditions: mutable.Set[(ConditionCount, Int)],
                                           binaryConditions: mutable.Set[Unit],
                                           joinLine: JoinLine,
                                           out: Collector[OUT]): Unit = {

    unaryConditions.foreach { lhsConditionCountWithHash =>
      val lhsConditionCount = lhsConditionCountWithHash._1
      if (shouldProcess(joinLine, lhsConditionCount)) {
        val lhsHash = lhsConditionCountWithHash._2
        // Find AR implied condition (if any).
        val arImpliedCondition = findImpliedCondition(lhsConditionCountWithHash._1.toCondition(this.testCondition))
        unaryConditions.foreach { rhsConditionCountWithHash =>
          val rhsConditionCount = rhsConditionCountWithHash._1
          if (arImpliedCondition == null || arImpliedCondition != rhsConditionCount.toCondition(this.testCondition)) {
            // Calculate the "ring-buffer" distance between the two hashes.
            val rhsHash = rhsConditionCountWithHash._2
            var lhsToRhs = rhsHash - lhsHash
            if (lhsToRhs < 0) lhsToRhs += NUM_HASH_VALUES

            // Find out if the LHS is "before" the RHS.
            if (((lhsToRhs > 0 && lhsToRhs < DEPENDENT_RANGE) ||
              // On tie, use secondary criteria.
              (lhsToRhs == DEPENDENT_RANGE && lhsHash < rhsHash) ||
              (lhsToRhs == 0 && lhsConditionCount < rhsConditionCount)) &&
              shouldAdd(rhsConditionCount)) {
              this.rhsConditionCounts += rhsConditionCount
            }
          }
        }

        output(lhsConditionCount, this.rhsConditionCounts, out)
        this.rhsConditionCounts.clear()
      }
    }
  }

  /** Tells whether to consider the given condition count as RHS for the LHS condition count. */
  def shouldAdd(rhsConditionCount: ConditionCount): Boolean = true

  def output(lhsConditionCount: ConditionCount, rhsConditionCounts: ArrayBuffer[ConditionCount], out: Collector[OUT])
}

object AbstractExtractBalancedUnaryUnaryOverlapCandidates {

  /**
   * We use only the lower bits of the hash function as especially for small strings, the #hashCode() function is skewed
   * on the higher bits.
   */
  val HASH_BIT_MASK = 0xFF

  val NUM_HASH_VALUES = HASH_BIT_MASK + 1

  val DEPENDENT_RANGE = NUM_HASH_VALUES >> 1

  implicit object ConditionCountWithHashOrdering extends Ordering[(ConditionCount, Int)] {
    override def compare(x: (ConditionCount, Int), y: (ConditionCount, Int)): Int = x._1.compare(y._1)
  }

}
