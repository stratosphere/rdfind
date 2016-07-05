package de.hpi.isg.sodap.rdfind.operators.candidate_extraction

import com.google.common.hash.BloomFilter
import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.operators.CreateDependencyCandidates
import de.hpi.isg.sodap.rdfind.util.BloomFilterParameters
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * This function creates CIND evidences for 1/1, 1/2, and 2/1 CINDs. If the referenced set becomes too large,
 * it will be converted into a Bloom filter.
 *
 * @author sebastian.kruse
 * @since 20.04.2015
 */
class CreateAlmostAllHalfApproximateCindCandidates(exactnessThreshold: Int,
                                             bloomFilterParameters: BloomFilterParameters[Condition],
                                             isUseAssociationRules: Boolean)
  extends CreateDependencyCandidates[HalfApproximateCindSet, Condition, Condition](true, true, isUseAssociationRules) {

  private lazy val refConditions = new ArrayBuffer[Condition]()

  private lazy val bloomFilter = bloomFilterParameters.createBloomFilter

  override protected def createUnaryConditions: mutable.Set[Condition] = mutable.SortedSet[Condition]()

  override protected def createBinaryConditions: mutable.Set[Condition] = mutable.SortedSet[Condition]()

  override protected def collectUnaryCapture(collector: mutable.Set[Condition], condition: Condition): Unit =
    collector += condition

  override def collectBinaryCaptures(collector: mutable.Set[Condition], condition: Condition): Unit =
    collector += condition

  override def splitAndCollectUnaryCaptures(collector: mutable.Set[Condition], condition: Condition): Unit = {
    val conditions = decodeConditionCode(condition.conditionType, isRequireDoubleCode = true)
    val newConditionCode1 = createConditionCode(conditions._1, secondaryCondition = conditions._3)
    collector += Condition(condition.conditionValue1, null, newConditionCode1)
    val newConditionCode2 = createConditionCode(conditions._2, secondaryCondition = conditions._3)
    collector += Condition(condition.conditionValue2, null, newConditionCode2)
  }


  // We do not need this function as we override its more specific sibling.
  override def collectDependencyCandidates(unaryConditions: mutable.Set[Condition],
                                           binaryConditions: mutable.Set[Condition],
                                           out: Collector[HalfApproximateCindSet]): Unit = ???

  override def collectDependencyCandidates(unaryConditions: mutable.Set[Condition],
                                           binaryConditions: mutable.Set[Condition],
                                           joinLine: JoinLine,
                                           out: Collector[HalfApproximateCindSet]): Unit = {

    val allConditions = unaryConditions ++ binaryConditions
    unaryConditions.foreach { dependentCondition =>
      if (shouldProcess(joinLine, dependentCondition)) {
        val arImpliedCondition = findImpliedCondition(dependentCondition)

        // Gather all potential referenced captures for the current capture.
        this.refConditions.clear()
        allConditions.foreach { referencedCondition =>
          if (!dependentCondition.implies(referencedCondition) && arImpliedCondition != referencedCondition) {
            this.refConditions += referencedCondition
          }
        }

        if (this.refConditions.size > this.exactnessThreshold) {
          // If there are too many referenced captures, put them into a Bloom filter.
          this.bloomFilter.clear()
          this.refConditions.foreach { refCondition =>
            bloomFilter.put(refCondition)
          }
          out.collect(HalfApproximateCindSet(dependentCondition.conditionType,
            dependentCondition.conditionValue1NotNull, dependentCondition.conditionValue2NotNull,
            1, bloomFilter.exportBits(), Array(), false))

        } else {
          // Otherwise, output a normal CIND evidence.
          out.collect(HalfApproximateCindSet(dependentCondition.conditionType,
            dependentCondition.conditionValue1NotNull, dependentCondition.conditionValue2NotNull,
            1, Array(), this.refConditions.toArray, true))

        }
      }
    }
  }
}




