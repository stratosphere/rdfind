package de.hpi.isg.sodap.rdfind.operators.candidate_extraction

import java.lang

import com.google.common.hash.BloomFilter
import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.operators.CreateApproximatedCindCandidates.{BloomFilterCindCandidatesInitializer, ExactCindCandidatesInitializer}
import de.hpi.isg.sodap.rdfind.operators.CreateDependencyCandidates
import de.hpi.isg.sodap.rdfind.operators.candidate_extraction.CreateApproximatedCindCandidates2.ActualCindsInitializer
import de.hpi.isg.sodap.rdfind.util.ConditionCodes._
import de.hpi.isg.sodap.rdfind.util.{BloomFilterParameters, ConditionCodes}
import org.apache.flink.api.common.functions.BroadcastVariableInitializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * This function create all CIND evidences from the given join lines that fulfill these two conditions:
 * <ol>
 * <li>It is a 2/1, 1/1, 1/2 CIND that is in the set of approximately-known CINDs.</li>
 * <li>It is a 2/2 CIND that is not pruned by a known 1/2 CIND.</li>
 * </ol>
 *
 * @author sebastian.kruse
 * @since 20.04.2015
 */
class CreateApproximatedCindCandidates2(bloomFilterParameters: BloomFilterParameters[Condition],
                                        exactnessThreshold: Int,
                                        isUseAssociationRules: Boolean)
  extends CreateDependencyCandidates[CindSet, Condition, Condition](true, true, isUseAssociationRules) {

  private lazy val refConditions = new ArrayBuffer[Condition]()

  private lazy val logger = LoggerFactory.getLogger(getClass)

  /** Maps dependent captures of approximate CIND sets to their referenced captures as Bloom filters. */
  private var bloomFilterCindCandidates: Map[Condition, (BloomFilter[Condition], Int)] = _

  /** Maps dependent captures of approximate, explicit CIND sets to their referenced captures. */
  private var exactCindCandidates: Map[Condition, (Set[Condition], Int)] = _

  /** Maps dependent captures of CINDs to the referenced captures. */
  private var actualCinds: Map[Condition, Set[Condition]] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // Load the exact CIND candidates.
    this.exactCindCandidates = this.getRuntimeContext.getBroadcastVariableWithInitializer(
      CreateApproximatedCindCandidates2.EXACT_CIND_CANDIDATES, new ExactCindCandidatesInitializer)
    this.logger.info("Loaded {} explicit CIND candidate sets with {} CIND candidates.",
      this.exactCindCandidates.size,
      this.exactCindCandidates.map(_._2._1.size).sum)

    // Load the Bloom-filter-based CIND candidates.
    this.bloomFilterCindCandidates = this.getRuntimeContext.getBroadcastVariableWithInitializer(
      CreateApproximatedCindCandidates2.BLOOM_FILTER_CIND_CANDIDATES,
      new BloomFilterCindCandidatesInitializer(bloomFilterParameters))
    this.logger.info("Loaded {} Bloom filter-based CIND candidate sets.",
      this.bloomFilterCindCandidates.size)

    // Load the already known CINDs.
    this.actualCinds = this.getRuntimeContext.getBroadcastVariableWithInitializer(
      CreateApproximatedCindCandidates2.ACTUAL_CINDS, new ActualCindsInitializer)
  }

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


  override def collectDependencyCandidates(unaryConditions: mutable.Set[Condition],
                                           binaryConditions: mutable.Set[Condition],
                                           out: Collector[CindSet]): Unit = ???

  override def collectDependencyCandidates(unaryConditions: mutable.Set[Condition],
                                           binaryConditions: mutable.Set[Condition],
                                           joinLine: JoinLine,
                                           out: Collector[CindSet]): Unit = {

    val allConditions = unaryConditions ++ binaryConditions

    // Collect the evidences for approximate CINDs.
    unaryConditions.foreach { dependentCondition =>

      if (shouldProcess(joinLine, dependentCondition)) {
        // At first, we need to see if the current dependent capture is part of an approximate CIND set.
        val exactCindCandidates = this.exactCindCandidates.getOrElse(dependentCondition, null)
        val bloomFilterCindCandidates = if (exactCindCandidates == null) {
          this.bloomFilterCindCandidates.getOrElse(dependentCondition, null)
        } else {
          null
        }

        // If so, we gather the potential referenced captures.
        if (exactCindCandidates != null || bloomFilterCindCandidates != null) {
          // Find out the already known support for the designated CIND candidates.
          val depCount = if (exactCindCandidates != null) exactCindCandidates._2 else bloomFilterCindCandidates._2

          // Find AR implied condition (if any).
          val arImpliedCondition = findImpliedCondition(dependentCondition)

          // Gather the referenced condititons.
          this.refConditions.clear()
          allConditions.foreach { referencedCondition =>
            if (!dependentCondition.implies(referencedCondition) && referencedCondition != arImpliedCondition) {
              this.refConditions += referencedCondition
            }
          }
          // We have to figure out, whether we produced an inexact CIND candidate representation beforehand.
          // If not, we do not need to reconsider these CIND candidates, since they are already accurately encoded in
          // the broadcast candidates. Exception: There was no explicit CIND candidate at all. Then we have the candidates
          // only given in terms of a Bloom filter.
          if (this.refConditions.size > this.exactnessThreshold) {
            // If so, we can cut down this originally large candidate set.
            val referencedConditions =
              if (exactCindCandidates != null) {
                this.refConditions.filter(exactCindCandidates._1).toArray
              } else {
                this.refConditions.filter { referencedCondition =>
                  bloomFilterCindCandidates._1.mightContain(referencedCondition)
                }.toArray
              }
            out.collect(CindSet(dependentCondition.conditionType,
              dependentCondition.conditionValue1NotNull, dependentCondition.conditionValue2NotNull,
              -depCount, referencedConditions))
          }
        }
      }
    }

    // Collect the evidences for 2/XXX CINDs.
    binaryConditions.foreach { dependentCondition =>

      this.refConditions.clear()
      val knownReferencedCaptures =
        this.actualCinds.getOrElse(dependentCondition.firstUnaryCondition, Set()) ++
          this.actualCinds.getOrElse(dependentCondition.secondUnaryCondition, Set())

      if (shouldProcess(joinLine, dependentCondition)) {
        allConditions.foreach { referencedCondition =>
          // Make sure that this CIND is not implied by a known CIND.
          if (!dependentCondition.implies(referencedCondition) && !knownReferencedCaptures(referencedCondition)) {
            this.refConditions += referencedCondition
          }
        }
      }

      out.collect(CindSet(dependentCondition.conditionType,
        dependentCondition.conditionValue1NotNull, dependentCondition.conditionValue2NotNull,
        1, this.refConditions.toArray))
    }
  }

}


object CreateApproximatedCindCandidates2 {

  val EXACT_CIND_CANDIDATES = "exact-cind-candidates"
  val BLOOM_FILTER_CIND_CANDIDATES = "bloom-filter-cind-candidates"
  val ACTUAL_CINDS = "actual-cinds"

  class ExactCindCandidatesInitializer extends BroadcastVariableInitializer[HalfApproximateCindSet, Map[Condition, (Set[Condition], Int)]] {

    override def initializeBroadcastVariable(iterable: lang.Iterable[HalfApproximateCindSet]): Map[Condition, (Set[Condition], Int)] = {
      iterable.toStream.map { halfApproximateCindSet =>
        (Condition(halfApproximateCindSet.depConditionValue1,
          decoalesce(halfApproximateCindSet.depCaptureType, halfApproximateCindSet.depConditionValue2),
          halfApproximateCindSet.depCaptureType),
          (halfApproximateCindSet.refConditions.to[Set], halfApproximateCindSet.depCount))
      }.toMap

    }

  }

  class BloomFilterCindCandidatesInitializer(bloomFilterParameters: BloomFilterParameters[Condition])
    extends BroadcastVariableInitializer[HalfApproximateCindSet, Map[Condition, (BloomFilter[Condition], Int)]] {

    override def initializeBroadcastVariable(iterable: lang.Iterable[HalfApproximateCindSet]): Map[Condition, (BloomFilter[Condition], Int)] = {
      iterable.toStream.map { halfApproximateCindSet =>
        val approximateRefConditions = bloomFilterParameters.createBloomFilter
        approximateRefConditions.wrap(halfApproximateCindSet.approximateRefConditions)
        (Condition(halfApproximateCindSet.depConditionValue1,
          decoalesce(halfApproximateCindSet.depCaptureType, halfApproximateCindSet.depConditionValue2),
          halfApproximateCindSet.depCaptureType),
          (approximateRefConditions, halfApproximateCindSet.depCount))
      }.toMap
    }
  }

  class ActualCindsInitializer
    extends BroadcastVariableInitializer[CindSet, Map[Condition, Set[Condition]]] {

    override def initializeBroadcastVariable(cindSets: lang.Iterable[CindSet]): Map[Condition, Set[Condition]] = {
      cindSets.map { cindSet =>
        (cindSet.depCondition.decoalesce(), cindSet.refConditions.flatMap { condition =>
          if (condition.isUnary)
            Seq(condition)
          else
            Seq(condition, condition.firstUnaryCondition, condition.secondUnaryCondition)
        }.map(_.decoalesce()).toSet)
      }.toMap
    }
  }

  def decoalesce(captureType: Int, conditionValue2: String) =
    if (ConditionCodes.isUnaryCondition(captureType)) null else conditionValue2


}

