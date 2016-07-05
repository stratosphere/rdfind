package de.hpi.isg.sodap.rdfind.operators

import com.google.common.hash.BloomFilter
import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.util.BloomFilterParameters
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * This function infers non-minimal 2/1-CINDs from 1/1-CINDs and -DVOs. This input should be grouped by the
 * right-hand side capture condition and value.
 *
 * @author sebastian.kruse
 * @since 20.04.2015
 */
class CreateAllHalfApproximateCindCandidates(exactnessThreshold: Int,
                                             bloomFilterParameters: BloomFilterParameters[Condition],
                                             isUseFrequentConditionsFilter: Boolean = false,
                                             isUseAssociationRules: Boolean,
                                             isUseFrequentCapturesBloomFilter: Boolean,
                                             splitStrategy: Int)
  extends CreateDependencyCandidates[HalfApproximateCindSet, Condition, Condition](true, true, isUseAssociationRules,
    isUsingFrequentCapturesBloomFilter = isUseFrequentCapturesBloomFilter) {

  private lazy val refConditions = new ArrayBuffer[Condition]()

  private var frequentBinaryConditions: Map[Int, BloomFilter[Condition]] = _

  private lazy val bloomFilter = bloomFilterParameters.createBloomFilter

  override protected def createUnaryConditions: mutable.Set[Condition] = mutable.SortedSet[Condition]()

  override protected def createBinaryConditions: mutable.Set[Condition] = mutable.SortedSet[Condition]()

  override protected def collectUnaryCapture(collector: mutable.Set[Condition], condition: Condition): Unit =
    if (isFrequentCapture(condition)) collector += condition

  override def collectBinaryCaptures(collector: mutable.Set[Condition], condition: Condition): Unit =
    if (isFrequentCapture(condition)) collector += condition

  override def splitAndCollectUnaryCaptures(collector: mutable.Set[Condition], condition: Condition): Unit = {
    val conditions = decodeConditionCode(condition.conditionType, isRequireDoubleCode = true)

    val newConditionCode1 = createConditionCode(conditions._1, secondaryCondition = conditions._3)
    var capture = Condition(condition.conditionValue1, null, newConditionCode1)
    if (isFrequentCapture(condition)) collector += capture

    val newConditionCode2 = createConditionCode(conditions._2, secondaryCondition = conditions._3)
    capture = Condition(condition.conditionValue2, null, newConditionCode2)
    if (isFrequentCapture(condition)) collector += capture
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
    splitStrategy match {

      case 1 => {
        // Hash-based partition of join line.
        allConditions.foreach { dependentCondition =>
          if (shouldProcess(joinLine, dependentCondition)) {
            processDependentCondition(allConditions, dependentCondition, out)
          }
        }
      }

      case 2 => {
        // Range-based parition of join line.
        val relevantRange = determineRelevantRange(joinLine, allConditions)
        allConditions.slice(from = relevantRange._1, until = relevantRange._2).foreach { dependentCondition =>
          processDependentCondition(allConditions, dependentCondition, out)
        }
      }

      case _ => throw new IllegalStateException(s"Unsupported split strategy: $splitStrategy")
    }
  }

  /**
   * Creates the CIND set for the dependent condition.
   */
  @inline
  private def processDependentCondition(allConditions: mutable.Set[Condition], dependentCondition: Condition, out: Collector[HalfApproximateCindSet]): Unit = {
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

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    if (this.isUseFrequentConditionsFilter) {
      this.frequentBinaryConditions = this.getRuntimeContext
        .getBroadcastVariable[(Int, BloomFilter[Condition])](CreateAllCindCandidatesApproximate.FREQUENT_BINARY_CONDITIONS_BROADCAST)
        .toMap
    }
  }

}




