package de.hpi.isg.sodap.rdfind.plan

import com.google.common.hash.BloomFilter
import de.hpi.isg.sodap.rdfind.data._
import de.hpi.isg.sodap.rdfind.operators._
import de.hpi.isg.sodap.rdfind.operators.candidate_extraction.{CreateAlmostAllHalfApproximateCindCandidates, CreateApproximatedCindCandidates2}
import de.hpi.isg.sodap.rdfind.operators.candidate_merging.{IntersectCindCandidates, IntersectHalfApproximateCindCandidates}
import de.hpi.isg.sodap.rdfind.plan.FrequentConditionPlanner.ConstructedDataSets
import de.hpi.isg.sodap.rdfind.programs.RDFind.Parameters
import de.hpi.isg.sodap.rdfind.util.BloomFilterParameters
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.util.Collector

/**
 * This traversal strategy finds the 1/1, 1/2, and 2/1 strategies aproximately at first. Then, in a second iteration,
 * these CINDs are verified (if necessary) and the 2/2 are also sought. However, there we can take already known
 * 1/2 CINDs into account for pruning.
 *
 * @author Sebastian Kruse
 * @since 14.04.2015.
 */
class LateBBTraversalStrategy extends TraversalStrategy {

  override def enhanceFlinkPlan(tripleJoin: DataSet[JoinLine],
                                frequentConditionDataSets: ConstructedDataSets,
                                frequentCapturesBloomFilter: DataSet[BloomFilter[Condition]],
                                parameters: Parameters): DataSet[Cind] = {

    if (frequentCapturesBloomFilter != null)
      throw new IllegalArgumentException("Frequent captures Bloom filter not supported in this traversal strategy.")

    if (parameters.rebalanceSplitStrategy != 1) throw new IllegalArgumentException("Only split strategy 1 is supported!")

    val minSupport = parameters.minSupport
    val isReevaluateBinaryCaptureFrequency = parameters.isUseFrequentItemSet && parameters.isCreateAnyBinaryCaptures
    if (isReevaluateBinaryCaptureFrequency) {
      throw new IllegalStateException("Lazy evaluation of binary conditions is not supported anymore.")
    }
    val exactnessThreshold = parameters.explicitCandidateThreshold
    val parallelism = parameters.stratosphereParameters.degreeOfParallelism

    val bloomFilterParameters = BloomFilterParameters[Condition](exactnessThreshold * 10, 0.1, new Condition.Funnel().getClass.getName)

    val createAlmostAllHalfApproximateCindCandidates = new CreateAlmostAllHalfApproximateCindCandidates(
      exactnessThreshold, bloomFilterParameters, isUseAssociationRules = parameters.isUseAssociationRules)


    // Create 1/XXX CIND evidences, partially as Bloom filters.
    val approximateCindCandidates = tripleJoin
      .flatMap(createAlmostAllHalfApproximateCindCandidates)
      .name("Create half approximated CIND evidences")
    if (isReevaluateBinaryCaptureFrequency) {
      approximateCindCandidates.withBroadcastSet(frequentConditionDataSets.frequentDoubleConditionBloomFilter,
        CreateAllCindCandidatesApproximate.FREQUENT_BINARY_CONDITIONS_BROADCAST)
    }
    if (parameters.isUseAssociationRules) {
      approximateCindCandidates.withBroadcastSet(frequentConditionDataSets.associationRules,
        CreateDependencyCandidates.ASSOCIATION_RULE_BROADCAST)
    }

    // Merge the CIND evidences.
    val approximateCinds = approximateCindCandidates
      // BUG: allowing this code makes Flink crash (at least locally)
      //      .groupBy("depCaptureType", "depConditionValue1", "depConditionValue2")
      //      .combineGroup(new IntersectCindCandidatesApproximate())
      .groupBy("depCaptureType", "depConditionValue1", "depConditionValue2")
      .reduceGroup(new IntersectHalfApproximateCindCandidates(bloomFilterParameters, parameters.mergeWindowSize))
      .name("Intersect half approximated CIND evidences")
      .filter { cindSet =>
      cindSet.depCount >= minSupport && // CIND set is frequent.
        (cindSet.approximateRefConditions.length > 0 || cindSet.refConditions.length > 0) // CIND set is not empty.
    }.name("Filter frequent half approximated CIND sets")

    // Filter out the CINDs.
    val firstRoundCindSets = approximateCinds
      .flatMap { (approximateCinds: HalfApproximateCindSet, out: Collector[CindSet]) =>
      if (approximateCinds.isExact) {
        out.collect(approximateCinds.toCindSet)
      }
    }.name("Filter first-round CIND sets")

    // Filter out the CIND candidates.
    val bloomFilterApproximateCinds = approximateCinds
      .filter { halfApproximateCindSet =>
      !halfApproximateCindSet.isExact && halfApproximateCindSet.approximateRefConditions.length > 0
    }.name("Filter Bloom filter approximated CIND sets")

    val explicitApproximateCinds = approximateCinds
      .filter { halfApproximateCindSet =>
      !halfApproximateCindSet.isExact && halfApproximateCindSet.approximateRefConditions.length == 0
    }.name("Filter explicit approximated CIND sets")

    val secondRoundCandidates = tripleJoin
      .flatMap(new CreateApproximatedCindCandidates2(bloomFilterParameters, exactnessThreshold,
      parameters.isUseAssociationRules))
      .withBroadcastSet(explicitApproximateCinds, CreateApproximatedCindCandidates2.EXACT_CIND_CANDIDATES)
      .withBroadcastSet(bloomFilterApproximateCinds, CreateApproximatedCindCandidates2.BLOOM_FILTER_CIND_CANDIDATES)
      .withBroadcastSet(firstRoundCindSets, CreateApproximatedCindCandidates2.ACTUAL_CINDS)
      .name("Create second-round CIND evidences")
    if (parameters.isUseAssociationRules) {
      secondRoundCandidates.withBroadcastSet(frequentConditionDataSets.associationRules,
        CreateDependencyCandidates.ASSOCIATION_RULE_BROADCAST)
    }

    val secondRoundCindSets = secondRoundCandidates
      // We set it combinable, so this right here should not be necessary.
      //      .groupBy("depCaptureType", "depConditionValue1", "depConditionValue2")
      //      .combineGroup(new IntersectCindCandidates(countMergeFunction = ApproximateAllAtOnceTraversalStrategy.assertEqualsAndFirst))
      //      .name("Intersect second-round CIND evidences")
      .groupBy("depCaptureType", "depConditionValue1", "depConditionValue2")
      .reduceGroup(new IntersectCindCandidates(parameters.mergeWindowSize,
      countMergeFunction = LateBBTraversalStrategy.addPositiveKeepNegative(_, _)))
      .name("Intersect second-round CIND evidences")
      .filter(_.refConditions.length > 0)
      .name("Remove empty CIND sets")
      .map { cindSet => cindSet.depCount = Math.abs(cindSet.depCount); cindSet }
      .name("Decode negative CIND support counts")
      .filter(_.depCount >= minSupport)
      .name("Remove infrequent CINDs")


    splitAndCleanCindSets(parameters, firstRoundCindSets, secondRoundCindSets)
  }

  /**
   * @return whether this strategy can incorporate frequent conditions.
   */
  override def isAbleToUseFrequentConditions: Boolean = true

  override def extractJoinLineSize: (JoinLine) => Int = _.numCombinedConditions

  override def extractMaximumJoinLineSize: (JoinLineLoad) => Int = _.combinedBlockSize
}

object LateBBTraversalStrategy {

  def addPositiveKeepNegative(a: Int, b: Int) = {
    if (a < 0 && a == b) a
    else if (a > 0 && b > 0) a + b
    else if (a == 0 || b == 0) a + b
    else throw new IllegalArgumentException(s"Unexpected input $a, $b.")
  }

}
