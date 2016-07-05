package de.hpi.isg.sodap.rdfind.operators

import com.google.common.hash.BloomFilter
import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.operators.candidate_extraction.CreateAllCindCandidates
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable

class CreateAllCindCandidatesApproximate(isUseFrequentConditionsFilter: Boolean = false,
                                          isUseAssociationRules: Boolean)
  extends CreateDependencyCandidates[ApproximateCindSet, Condition, Condition](true, true, isUseAssociationRules) {

  private var frequentBinaryConditions: Map[Int, BloomFilter[Condition]] = _

  override protected def createUnaryConditions: mutable.Set[Condition] = new mutable.HashSet[Condition]()

  override protected def createBinaryConditions: mutable.Set[Condition] = new mutable.HashSet[Condition]()

  override protected def collectUnaryCapture(collector: mutable.Set[Condition], condition: Condition): Unit =
    collector += condition

  override def collectBinaryCaptures(collector: mutable.Set[Condition], condition: Condition): Unit =
    if (!this.isUseFrequentConditionsFilter || {
      val filter = this.frequentBinaryConditions(condition.conditionType)
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
                                           out: Collector[ApproximateCindSet]): Unit = {

    val allConditions = unaryConditions ++ binaryConditions
    allConditions.foreach { dependentCondition =>
      val impliedCondition = findImpliedCondition(dependentCondition)
      var numReferencedConditions = 0
      val bloomFilter = BloomFilter.create[Condition](new Condition.Funnel(), 1000, 0.1)
      allConditions.foreach { referencedCondition =>
        if (!dependentCondition.implies(referencedCondition) && referencedCondition != impliedCondition) {
          bloomFilter.put(referencedCondition)
          numReferencedConditions += 1
        }
      }
      out.collect(ApproximateCindSet(dependentCondition.conditionType,
        dependentCondition.conditionValue1NotNull,
        dependentCondition.conditionValue2NotNull,
        1,
        if (numReferencedConditions == 0) null else bloomFilter))
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

object CreateAllCindCandidatesApproximate {

  val FREQUENT_BINARY_CONDITIONS_BROADCAST = CreateAllCindCandidates.FREQUENT_BINARY_CONDITIONS_BROADCAST

}


