package de.hpi.isg.sodap.rdfind.operators

import java.lang

import com.google.common.hash.BloomFilter
import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.util.ConditionCodes
import org.apache.flink.api.common.functions.{BroadcastVariableInitializer, RichFlatMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * @tparam T output type
 * @tparam UC type of the collected unary captures
 * @tparam BC type of the collected binary captures
 * @author sebastian.kruse
 * @since 08.05.2015
 */
abstract class CreateDependencyCandidates[T, UC, BC](isUsingUnaryCaptures: Boolean,
                                                     isUsingBinaryCaptures: Boolean,
                                                     isLoadingAssociationRules: Boolean,
                                                     isUsingFrequentCapturesBloomFilter: Boolean = false)
  extends RichFlatMapFunction[JoinLine, T] {

  protected lazy val unaryConditions = createUnaryConditions

  protected lazy val binaryConditions = createBinaryConditions

  protected var associationRuleImpliedCinds: Map[Condition, Condition] = _

  protected var frequentCapturesBloomFilter: BloomFilter[Condition] = _

  protected def createUnaryConditions: mutable.Set[UC]

  protected def createBinaryConditions: mutable.Set[BC]

  protected def collectUnaryCapture(collector: mutable.Set[UC], condition: Condition)

  def collectBinaryCaptures(collector: mutable.Set[BC], condition: Condition)

  def splitAndCollectUnaryCaptures(collector: mutable.Set[UC], condition: Condition)

  def collectDependencyCandidates(unaryConditions: mutable.Set[UC],
                                  binaryConditions: mutable.Set[BC],
                                  out: Collector[T]): Unit

  def collectDependencyCandidates(unaryConditions: mutable.Set[UC],
                                  binaryConditions: mutable.Set[BC],
                                  joinLine: JoinLine,
                                  out: Collector[T]): Unit = collectDependencyCandidates(unaryConditions, binaryConditions, out)


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    if (isLoadingAssociationRules) {
      this.associationRuleImpliedCinds =
        getRuntimeContext.getBroadcastVariableWithInitializer[AssociationRule, Map[Condition, Condition]](
          CreateDependencyCandidates.ASSOCIATION_RULE_BROADCAST,
          CreateDependencyCandidates.AssocationRuleBroadcastInitializer)
      LoggerFactory.getLogger(getClass).info(s"Loaded ${this.associationRuleImpliedCinds.size} association rules.")
    }

    if (isUsingFrequentCapturesBloomFilter) {
      this.frequentCapturesBloomFilter =
        getRuntimeContext.getBroadcastVariable[BloomFilter[Condition]](
          CreateDependencyCandidates.FREQUENT_CAPTURES_BROADCAST
        )(0)
    }
  }

  /**
   * This method can be used by subclasses to test if a capture is in the frequent condition Bloom filter.
   */
  protected def isFrequentCapture(capture: Condition) =
    !isUsingFrequentCapturesBloomFilter || this.frequentCapturesBloomFilter.mightContain(capture)

  override def flatMap(joinLine: JoinLine, out: Collector[T]): Unit = {
    // XXX: Debug instrumentation. No crucial functionality!
    val startTime = System.currentTimeMillis

    if (this.isUsingUnaryCaptures) this.unaryConditions.clear()
    if (this.isUsingBinaryCaptures) this.binaryConditions.clear()

    // Gather the captures from the join line.
    joinLine.conditions.foreach {
      condition =>
        if (!condition.isDoubleCondition) {
          // Unary capture encountered.
          if (this.isUsingUnaryCaptures) {
            collectUnaryCapture(this.unaryConditions, condition)
          }
        } else {
          if (this.isUsingBinaryCaptures) {
            collectBinaryCaptures(this.binaryConditions, condition)
          }
          if (this.isUsingUnaryCaptures) {
            splitAndCollectUnaryCaptures(this.unaryConditions, condition)
          }
        }
    }

    collectDependencyCandidates(
      if (this.isUsingUnaryCaptures) this.unaryConditions else null,
      if (this.isUsingBinaryCaptures) this.binaryConditions else null,
      joinLine,
      out)

    val endTime = System.currentTimeMillis
    val durationSeconds = (endTime - startTime) / 1000
    if (durationSeconds >= 1) {
      val numUnaryCaptures = if (this.isUsingUnaryCaptures) this.unaryConditions.size else 0
      val numBinaryCaptures = if (this.isUsingBinaryCaptures) this.binaryConditions.size else 0

      LoggerFactory.getLogger(getClass).warn(s"Took ${durationSeconds}s to create dependency candidates for " +
        s"${numUnaryCaptures + numBinaryCaptures} captures ($numUnaryCaptures unary, $numBinaryCaptures binary)")
    }

  }

  protected def findImpliedCondition(condition: Condition): Condition = {
    if (this.associationRuleImpliedCinds != null && condition.isUnary)
      this.associationRuleImpliedCinds.getOrElse(condition, null)
    else
      null
  }

  /**
   * Helper function for load balancing: tells whether this dependent condition should be processed for this join
   * line split.
   */
  def shouldProcess[X](joinLine: JoinLine, dependentCondition: X): Boolean = {
    val result = (joinLine.partitionKey == -1) || {
      val responsibleKey = Math.abs(dependentCondition.hashCode()) % joinLine.numPartitions
      responsibleKey == joinLine.partitionKey
    }

    result
  }

  def determineRelevantRange[X](joinLine: JoinLine, captures: scala.collection.Set[X]) =
    if (joinLine.partitionKey == -1) {
      (0, captures.size)
    } else {
      // I like the idea of this range, but it is not compatible to #slice(from, to).
//      joinLine.partitionKey to captures.size by this.parallelism
      val avgElementsPerParallelUnit = (captures.size + joinLine.numPartitions - 1) / joinLine.numPartitions // Make sure to round up!
      (avgElementsPerParallelUnit * joinLine.partitionKey,
        Math.min(avgElementsPerParallelUnit * (joinLine.partitionKey + 1), captures.size))
    }

}

object CreateDependencyCandidates {

  val ASSOCIATION_RULE_BROADCAST = "association-rules"

  val FREQUENT_CAPTURES_BROADCAST = "frequent-captures"

  object AssocationRuleBroadcastInitializer extends BroadcastVariableInitializer[AssociationRule, Map[Condition, Condition]] {


    override def initializeBroadcastVariable(associationRules: lang.Iterable[AssociationRule]): Map[Condition, Condition] = {
      associationRules.map { associationRule =>
        val conditionCode = associationRule.antecedentType | associationRule.consequentType
        val captureCode = ConditionCodes.addSecondaryConditions(conditionCode)
        val projectionCode = captureCode & ~conditionCode
        val mapEntry = Condition(associationRule.antecedent, null, associationRule.antecedentType | projectionCode) ->
          Condition(associationRule.consequent, null, associationRule.consequentType | projectionCode)
        mapEntry
      }.toMap
    }

  }

}